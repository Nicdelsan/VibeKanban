//! Normalizes Codex app-server v2 JSON-RPC traffic into conversation entries.

use std::{collections::HashMap, path::Path, sync::Arc};

use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use workspace_utils::{
    approvals::ApprovalStatus, diff::normalize_unified_diff, msg_store::MsgStore,
    path::make_path_relative,
};

use super::protocol::{
    CommandAction, ErrorNotification, FileUpdateChange, ItemDeltaNotification, ItemNotification,
    ItemStatus, JsonRpcMessage, McpToolCallResult, MessageNotification, PatchChangeKind,
    PlanUpdatedNotification, ThreadItem, ThreadResponse, TokenUsageNotification,
    TurnCompletedNotification,
};
use crate::{
    approvals::ToolCallMetadata,
    logs::{
        ActionType, CommandExitStatus, CommandRunResult, FileChange, NormalizedEntry,
        NormalizedEntryError, NormalizedEntryType, TodoItem, TokenUsageInfo, ToolResult,
        ToolResultValueType, ToolStatus,
        stderr_processor::normalize_stderr_logs,
        utils::{
            EntryIndexProvider,
            patch::{add_normalized_entry, replace_normalized_entry, upsert_normalized_entry},
        },
    },
};

pub fn normalize_logs(msg_store: Arc<MsgStore>, worktree_path: &Path) {
    let entry_index = EntryIndexProvider::start_from(&msg_store);
    normalize_stderr_logs(msg_store.clone(), entry_index.clone());

    let worktree = worktree_path.to_string_lossy().to_string();
    tokio::spawn(async move {
        let mut processor = CodexLogProcessor::new(msg_store.clone(), entry_index, worktree);
        let mut stdout_lines = msg_store.stdout_lines_stream();
        while let Some(Ok(line)) = stdout_lines.next().await {
            processor.process_line(&line);
        }
    });
}

/// Streaming text (assistant message or reasoning) keyed by item id.
struct StreamingText {
    index: usize,
    content: String,
}

struct CommandState {
    index: usize,
    command: String,
    output: String,
    exit_code: Option<i32>,
    status: ToolStatus,
    call_id: String,
}

impl CommandState {
    fn to_entry(&self) -> NormalizedEntry {
        let output = Some(self.output.trim())
            .filter(|o| !o.is_empty())
            .map(str::to_string);
        tool_entry(
            "bash",
            ActionType::CommandRun {
                command: self.command.clone(),
                result: Some(CommandRunResult {
                    exit_status: self
                        .exit_code
                        .map(|code| CommandExitStatus::ExitCode { code }),
                    output,
                }),
            },
            self.status.clone(),
            self.command.clone(),
            Some(&self.call_id),
        )
    }
}

struct PatchEntryState {
    index: usize,
    path: String,
    changes: Vec<FileChange>,
}

struct ToolState {
    index: usize,
    tool_name: String,
    display: String,
    arguments: Value,
}

pub struct CodexLogProcessor {
    msg_store: Arc<MsgStore>,
    entry_index: EntryIndexProvider,
    worktree: String,
    texts: HashMap<String, StreamingText>,
    commands: HashMap<String, CommandState>,
    patches: HashMap<String, Vec<PatchEntryState>>,
    tools: HashMap<String, ToolState>,
    compaction_reported: bool,
}

impl CodexLogProcessor {
    pub fn new(
        msg_store: Arc<MsgStore>,
        entry_index: EntryIndexProvider,
        worktree: String,
    ) -> Self {
        Self {
            msg_store,
            entry_index,
            worktree,
            texts: HashMap::new(),
            commands: HashMap::new(),
            patches: HashMap::new(),
            tools: HashMap::new(),
            compaction_reported: false,
        }
    }

    pub fn process_line(&mut self, line: &str) {
        if let Ok(error) = serde_json::from_str::<Error>(line) {
            self.add(error.to_normalized_entry());
            return;
        }
        if let Ok(approval) = serde_json::from_str::<Approval>(line) {
            if let Some(entry) = approval.to_normalized_entry_opt() {
                self.add(entry);
            }
            return;
        }
        match JsonRpcMessage::parse(line) {
            Some(JsonRpcMessage::Response(response)) => self.on_response(response.result),
            Some(JsonRpcMessage::Notification(notification)) => self.on_notification(
                &notification.method,
                notification.params.unwrap_or(Value::Null),
            ),
            Some(JsonRpcMessage::Error(error)) => {
                self.add(error_entry(error.error.message));
            }
            // Approval requests surface through the approval service and the `Approval`
            // lines written by the client.
            Some(JsonRpcMessage::Request(_)) | None => {}
        }
    }

    fn add(&self, entry: NormalizedEntry) -> usize {
        add_normalized_entry(&self.msg_store, &self.entry_index, entry)
    }

    fn replace(&self, index: usize, entry: NormalizedEntry) {
        replace_normalized_entry(&self.msg_store, index, entry);
    }

    fn on_response(&mut self, result: Value) {
        let Ok(thread) = serde_json::from_value::<ThreadResponse>(result) else {
            return;
        };
        self.msg_store.push_session_id(thread.thread.id);
        if let Some(model) = thread.model {
            let mut content = format!("model: {model}");
            if let Some(effort) = thread.reasoning_effort {
                content.push_str(&format!("  reasoning effort: {effort}"));
            }
            self.add(system_entry(content));
        }
    }

    fn on_notification(&mut self, method: &str, params: Value) {
        match method {
            "item/started" => {
                if let Some(n) = parse::<ItemNotification>(params) {
                    self.on_item_started(n.item);
                }
            }
            "item/completed" => {
                if let Some(n) = parse::<ItemNotification>(params) {
                    self.on_item_completed(n.item);
                }
            }
            "item/agentMessage/delta" => {
                if let Some(d) = parse::<ItemDeltaNotification>(params) {
                    self.append_text(&d.item_id, &d.delta, NormalizedEntryType::AssistantMessage);
                }
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                if let Some(d) = parse::<ItemDeltaNotification>(params) {
                    self.append_text(&d.item_id, &d.delta, NormalizedEntryType::Thinking);
                }
            }
            "item/commandExecution/outputDelta" => {
                if let Some(d) = parse::<ItemDeltaNotification>(params) {
                    self.append_command_output(&d.item_id, &d.delta);
                }
            }
            "turn/started" => self.compaction_reported = false,
            "turn/plan/updated" => {
                if let Some(plan) = parse::<PlanUpdatedNotification>(params) {
                    self.add(plan_entry(plan));
                }
            }
            "thread/tokenUsage/updated" => {
                if let Some(usage) = parse::<TokenUsageNotification>(params) {
                    self.add(token_usage_entry(&usage));
                }
            }
            "thread/compacted" => self.report_compaction(),
            "turn/completed" => {
                if let Some(error) =
                    parse::<TurnCompletedNotification>(params).and_then(|done| done.turn.error)
                {
                    self.add(error_entry(error.message));
                }
            }
            "error" => {
                if let Some(n) = parse::<ErrorNotification>(params) {
                    self.add(if n.will_retry {
                        system_entry(format!("Retrying: {}", n.error.message))
                    } else {
                        error_entry(n.error.message)
                    });
                }
            }
            "warning" | "configWarning" | "guardianWarning" => {
                if let Some(text) =
                    parse::<MessageNotification>(params).and_then(|n| n.message.or(n.summary))
                {
                    self.add(system_entry(text));
                }
            }
            "hook/completed" => {
                if let Some(text) = failed_hook_message(&params) {
                    self.add(system_entry(text));
                }
            }
            _ => {}
        }
    }

    fn report_compaction(&mut self) {
        if !self.compaction_reported {
            self.compaction_reported = true;
            self.add(system_entry("Context compacted".to_string()));
        }
    }

    fn append_text(&mut self, item_id: &str, delta: &str, entry_type: NormalizedEntryType) {
        if delta.is_empty() {
            return;
        }
        let (index, is_new, content) = match self.texts.get_mut(item_id) {
            Some(text) => {
                text.content.push_str(delta);
                (text.index, false, text.content.clone())
            }
            None => {
                let index = self.entry_index.next();
                self.texts.insert(
                    item_id.to_string(),
                    StreamingText {
                        index,
                        content: delta.to_string(),
                    },
                );
                (index, true, delta.to_string())
            }
        };
        upsert_normalized_entry(
            &self.msg_store,
            index,
            text_entry(entry_type, content),
            is_new,
        );
    }

    /// Replace streamed text with the final item text (or add it when nothing was streamed).
    fn finish_text(&mut self, item_id: &str, text: String, entry_type: NormalizedEntryType) {
        let streamed = self.texts.remove(item_id);
        if text.trim().is_empty() {
            return;
        }
        match streamed {
            Some(streamed) => self.replace(streamed.index, text_entry(entry_type, text)),
            None => {
                self.add(text_entry(entry_type, text));
            }
        }
    }

    fn on_item_started(&mut self, item: ThreadItem) {
        match item {
            ThreadItem::CommandExecution {
                id,
                command,
                command_actions,
                ..
            } => {
                let mut state = CommandState {
                    index: 0,
                    command: display_command(&command, &command_actions),
                    output: String::new(),
                    exit_code: None,
                    status: ToolStatus::Created,
                    call_id: id.clone(),
                };
                state.index = self.add(state.to_entry());
                self.commands.insert(id, state);
            }
            ThreadItem::FileChange { id, changes, .. } => {
                let entries = self.file_change_entries(&id, &changes, ToolStatus::Created);
                self.patches.insert(id, entries);
            }
            ThreadItem::McpToolCall {
                id,
                server,
                tool,
                arguments,
                ..
            } => {
                let tool_name = format!("mcp:{server}:{tool}");
                self.start_tool(id, tool_name, tool, arguments);
            }
            ThreadItem::DynamicToolCall {
                id,
                tool,
                arguments,
                ..
            } => self.start_tool(id, tool.clone(), tool, arguments),
            ThreadItem::WebSearch { id, query } => {
                let index = self.add(web_search_entry(&query, ToolStatus::Created));
                self.tools.insert(
                    id,
                    ToolState {
                        index,
                        tool_name: "web_search".to_string(),
                        display: query,
                        arguments: Value::Null,
                    },
                );
            }
            _ => {}
        }
    }

    fn start_tool(&mut self, id: String, tool_name: String, display: String, arguments: Value) {
        let state = ToolState {
            index: 0,
            tool_name,
            display,
            arguments,
        };
        let index = self.add(generic_tool_entry(&state, None, ToolStatus::Created));
        self.tools.insert(id, ToolState { index, ..state });
    }

    fn on_item_completed(&mut self, item: ThreadItem) {
        match item {
            ThreadItem::AgentMessage { id, text } => {
                self.finish_text(&id, text, NormalizedEntryType::AssistantMessage)
            }
            ThreadItem::Reasoning {
                id,
                summary,
                content,
            } => {
                let parts = if summary.is_empty() { content } else { summary };
                self.finish_text(&id, parts.join("\n\n"), NormalizedEntryType::Thinking);
            }
            ThreadItem::Plan { text, .. } if !text.trim().is_empty() => {
                self.add(tool_entry(
                    "plan",
                    ActionType::PlanPresentation { plan: text },
                    ToolStatus::Success,
                    "Plan".to_string(),
                    None,
                ));
            }
            ThreadItem::CommandExecution {
                id,
                command,
                command_actions,
                aggregated_output,
                exit_code,
                status,
            } => self.complete_command(
                id,
                display_command(&command, &command_actions),
                aggregated_output,
                exit_code,
                status,
            ),
            ThreadItem::FileChange {
                id,
                changes,
                status,
            } => self.complete_file_change(id, &changes, status),
            ThreadItem::McpToolCall {
                id,
                result,
                error,
                status,
                ..
            } => {
                let tool_result = mcp_tool_result(result.as_ref(), error.as_ref());
                self.complete_tool(&id, tool_result, tool_status(status));
            }
            ThreadItem::DynamicToolCall {
                id,
                success,
                status,
                ..
            } => {
                let status = match success {
                    Some(false) => ToolStatus::Failed,
                    _ => tool_status(status),
                };
                self.complete_tool(&id, None, status);
            }
            ThreadItem::WebSearch { id, query } => match self.tools.remove(&id) {
                Some(tool) => {
                    let query = if query.is_empty() {
                        tool.display
                    } else {
                        query
                    };
                    self.replace(tool.index, web_search_entry(&query, ToolStatus::Success));
                }
                None => {
                    self.add(web_search_entry(&query, ToolStatus::Success));
                }
            },
            ThreadItem::ContextCompaction { .. } => self.report_compaction(),
            ThreadItem::EnteredReviewMode { .. } => {
                self.add(system_entry("Review started".to_string()));
            }
            ThreadItem::ExitedReviewMode { review, .. } if !review.trim().is_empty() => {
                self.add(text_entry(NormalizedEntryType::AssistantMessage, review));
            }
            _ => {}
        }
    }

    fn append_command_output(&mut self, item_id: &str, delta: &str) {
        let Some(state) = self.commands.get_mut(item_id) else {
            return;
        };
        state.output.push_str(delta);
        let (index, entry) = (state.index, state.to_entry());
        self.replace(index, entry);
    }

    fn complete_command(
        &mut self,
        id: String,
        command: String,
        aggregated_output: Option<String>,
        exit_code: Option<i32>,
        status: ItemStatus,
    ) {
        let (mut state, is_new) = match self.commands.remove(&id) {
            Some(state) => (state, false),
            None => (
                CommandState {
                    index: self.entry_index.next(),
                    command,
                    output: String::new(),
                    exit_code: None,
                    status: ToolStatus::Created,
                    call_id: id,
                },
                true,
            ),
        };
        if let Some(output) = aggregated_output {
            state.output = output;
        }
        state.exit_code = exit_code;
        state.status = match status {
            ItemStatus::Completed if exit_code.is_some_and(|c| c != 0) => ToolStatus::Failed,
            other => tool_status(other),
        };
        upsert_normalized_entry(&self.msg_store, state.index, state.to_entry(), is_new);
    }

    fn complete_file_change(
        &mut self,
        id: String,
        changes: &[FileUpdateChange],
        status: ItemStatus,
    ) {
        let status = tool_status(status);
        match self.patches.remove(&id) {
            Some(entries) => {
                for entry in entries {
                    self.replace(
                        entry.index,
                        file_edit_entry(&entry.path, entry.changes, status.clone(), &id),
                    );
                }
            }
            None => {
                self.file_change_entries(&id, changes, status);
            }
        }
    }

    fn file_change_entries(
        &self,
        id: &str,
        changes: &[FileUpdateChange],
        status: ToolStatus,
    ) -> Vec<PatchEntryState> {
        changes
            .iter()
            .map(|change| {
                let (path, file_changes) = normalize_file_change(&self.worktree, change);
                let index = self.add(file_edit_entry(
                    &path,
                    file_changes.clone(),
                    status.clone(),
                    id,
                ));
                PatchEntryState {
                    index,
                    path,
                    changes: file_changes,
                }
            })
            .collect()
    }

    fn complete_tool(&mut self, id: &str, result: Option<ToolResult>, status: ToolStatus) {
        if let Some(tool) = self.tools.remove(id) {
            self.replace(tool.index, generic_tool_entry(&tool, result, status));
        }
    }
}

fn parse<T: DeserializeOwned>(params: Value) -> Option<T> {
    serde_json::from_value(params).ok()
}

fn tool_status(status: ItemStatus) -> ToolStatus {
    match status {
        ItemStatus::Completed => ToolStatus::Success,
        ItemStatus::Failed => ToolStatus::Failed,
        ItemStatus::Declined => ToolStatus::Denied { reason: None },
        ItemStatus::InProgress | ItemStatus::Unknown => ToolStatus::Created,
    }
}

/// Prefer the parsed command actions over the raw shell invocation (which on Windows is a
/// `pwsh.exe -Command "..."` wrapper).
fn display_command(command: &str, actions: &[CommandAction]) -> String {
    let parsed: Vec<&str> = actions
        .iter()
        .filter_map(|action| action.command.as_deref())
        .filter(|c| !c.trim().is_empty())
        .collect();
    if !parsed.is_empty() && parsed.len() == actions.len() {
        parsed.join(" && ")
    } else {
        command.to_string()
    }
}

fn normalize_file_change(worktree: &str, change: &FileUpdateChange) -> (String, Vec<FileChange>) {
    let relative = make_path_relative(&change.path, worktree);
    let changes = match &change.kind {
        PatchChangeKind::Add => vec![FileChange::Write {
            content: change.diff.clone(),
        }],
        PatchChangeKind::Delete => vec![FileChange::Delete],
        PatchChangeKind::Update { move_path } => {
            let rename = move_path.as_ref().map(|dest| FileChange::Rename {
                new_path: make_path_relative(dest, worktree),
            });
            let edit = FileChange::Edit {
                unified_diff: normalize_unified_diff(&relative, &change.diff),
                has_line_numbers: true,
            };
            rename.into_iter().chain(std::iter::once(edit)).collect()
        }
        PatchChangeKind::Unknown => vec![],
    };
    (relative, changes)
}

fn mcp_tool_result(
    result: Option<&McpToolCallResult>,
    error: Option<&Value>,
) -> Option<ToolResult> {
    if let Some(error) = error.filter(|e| !e.is_null()) {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| error.to_string());
        return Some(markdown_result(message));
    }
    let result = result?;
    let all_text = result
        .content
        .iter()
        .all(|block| block.get("type").and_then(Value::as_str) == Some("text"));
    if all_text {
        let text = result
            .content
            .iter()
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        return Some(markdown_result(text));
    }
    Some(ToolResult {
        r#type: ToolResultValueType::Json,
        value: result
            .structured_content
            .clone()
            .unwrap_or_else(|| Value::Array(result.content.clone())),
    })
}

fn markdown_result(text: String) -> ToolResult {
    ToolResult {
        r#type: ToolResultValueType::Markdown,
        value: Value::String(text),
    }
}

fn failed_hook_message(params: &Value) -> Option<String> {
    let run = params.get("run")?;
    if run.get("status").and_then(Value::as_str) != Some("failed") {
        return None;
    }
    let event = run
        .get("eventName")
        .and_then(Value::as_str)
        .unwrap_or("hook");
    let details = run
        .get("entries")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|e| e.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("; ")
        })
        .filter(|d| !d.is_empty())
        .map(|d| format!(": {d}"))
        .unwrap_or_default();
    Some(format!("Hook {event} failed{details}"))
}

fn text_entry(entry_type: NormalizedEntryType, content: String) -> NormalizedEntry {
    NormalizedEntry {
        timestamp: None,
        entry_type,
        content,
        metadata: None,
    }
}

fn system_entry(content: String) -> NormalizedEntry {
    text_entry(NormalizedEntryType::SystemMessage, content)
}

fn error_entry(content: String) -> NormalizedEntry {
    text_entry(
        NormalizedEntryType::ErrorMessage {
            error_type: NormalizedEntryError::Other,
        },
        content,
    )
}

fn tool_entry(
    tool_name: &str,
    action_type: ActionType,
    status: ToolStatus,
    content: String,
    call_id: Option<&str>,
) -> NormalizedEntry {
    NormalizedEntry {
        timestamp: None,
        entry_type: NormalizedEntryType::ToolUse {
            tool_name: tool_name.to_string(),
            action_type,
            status,
        },
        content,
        metadata: call_id.and_then(|id| {
            serde_json::to_value(ToolCallMetadata {
                tool_call_id: id.to_string(),
            })
            .ok()
        }),
    }
}

fn file_edit_entry(
    path: &str,
    changes: Vec<FileChange>,
    status: ToolStatus,
    call_id: &str,
) -> NormalizedEntry {
    tool_entry(
        "edit",
        ActionType::FileEdit {
            path: path.to_string(),
            changes,
        },
        status,
        path.to_string(),
        Some(call_id),
    )
}

fn generic_tool_entry(
    tool: &ToolState,
    result: Option<ToolResult>,
    status: ToolStatus,
) -> NormalizedEntry {
    tool_entry(
        &tool.tool_name,
        ActionType::Tool {
            tool_name: tool.tool_name.clone(),
            arguments: Some(tool.arguments.clone()).filter(|a| !a.is_null()),
            result,
        },
        status,
        tool.display.clone(),
        None,
    )
}

fn web_search_entry(query: &str, status: ToolStatus) -> NormalizedEntry {
    let query = if query.is_empty() { "..." } else { query };
    tool_entry(
        "web_search",
        ActionType::Search {
            query: query.to_string(),
        },
        status,
        query.to_string(),
        None,
    )
}

fn plan_entry(plan: PlanUpdatedNotification) -> NormalizedEntry {
    let todos: Vec<TodoItem> = plan
        .plan
        .iter()
        .map(|step| TodoItem {
            content: step.step.clone(),
            status: todo_status(&step.status).to_string(),
            priority: None,
        })
        .collect();
    let content = plan
        .explanation
        .as_deref()
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| match todos.len() {
            0 => "Plan updated".to_string(),
            n => format!("Plan updated ({n} steps)"),
        });
    tool_entry(
        "plan",
        ActionType::TodoManagement {
            todos,
            operation: "update".to_string(),
        },
        ToolStatus::Success,
        content,
        None,
    )
}

fn todo_status(status: &str) -> &'static str {
    match status {
        "inProgress" | "in_progress" => "in_progress",
        "completed" => "completed",
        _ => "pending",
    }
}

fn token_usage_entry(usage: &TokenUsageNotification) -> NormalizedEntry {
    let total = usage.token_usage.last.total_tokens;
    let window = usage.token_usage.model_context_window.unwrap_or_default();
    text_entry(
        NormalizedEntryType::TokenUsageInfo(TokenUsageInfo {
            total_tokens: u32::try_from(total).unwrap_or(u32::MAX),
            model_context_window: u32::try_from(window).unwrap_or(u32::MAX),
        }),
        format!("Tokens used: {total} / Context window: {window}"),
    )
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Error {
    LaunchError { error: String },
    AuthRequired { error: String },
}

impl Error {
    pub fn launch_error(error: String) -> Self {
        Self::LaunchError { error }
    }

    pub fn auth_required(error: String) -> Self {
        Self::AuthRequired { error }
    }

    pub fn raw(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    fn to_normalized_entry(&self) -> NormalizedEntry {
        match self {
            Error::LaunchError { error } => error_entry(error.clone()),
            Error::AuthRequired { error } => text_entry(
                NormalizedEntryType::ErrorMessage {
                    error_type: NormalizedEntryError::SetupRequired,
                },
                error.clone(),
            ),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Approval {
    ApprovalResponse {
        call_id: String,
        tool_name: String,
        approval_status: ApprovalStatus,
    },
}

impl Approval {
    pub fn approval_response(
        call_id: String,
        tool_name: String,
        approval_status: ApprovalStatus,
    ) -> Self {
        Self::ApprovalResponse {
            call_id,
            tool_name,
            approval_status,
        }
    }

    pub fn raw(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn display_tool_name(&self) -> String {
        let Self::ApprovalResponse { tool_name, .. } = self;
        match tool_name.as_str() {
            "codex.exec_command" => "Exec Command".to_string(),
            "codex.apply_patch" => "Edit".to_string(),
            other => other.to_string(),
        }
    }

    fn to_normalized_entry_opt(&self) -> Option<NormalizedEntry> {
        let Self::ApprovalResponse {
            approval_status, ..
        } = self;
        let tool_name = self.display_tool_name();
        match approval_status {
            ApprovalStatus::Pending | ApprovalStatus::Approved => None,
            ApprovalStatus::Denied { reason } => Some(text_entry(
                NormalizedEntryType::UserFeedback {
                    denied_tool: tool_name,
                },
                reason
                    .clone()
                    .unwrap_or_else(|| "User denied this tool use request".to_string())
                    .trim()
                    .to_string(),
            )),
            ApprovalStatus::TimedOut => Some(error_entry(format!(
                "Approval timed out for tool {tool_name}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use workspace_utils::log_msg::LogMsg;

    use super::*;
    use crate::logs::utils::patch::extract_normalized_entry_from_patch;

    const FIXTURE_0_169: &str = include_str!("fixtures/app_server_0_169.jsonl");

    /// Run lines through the processor; return the final state of each entry and the
    /// session ids that were pushed.
    fn run(lines: &[&str]) -> (Vec<NormalizedEntry>, Vec<String>) {
        let store = Arc::new(MsgStore::new());
        let mut processor = CodexLogProcessor::new(
            store.clone(),
            EntryIndexProvider::test_new(),
            "/repo".into(),
        );
        for line in lines {
            processor.process_line(line);
        }
        let mut entries: Vec<Option<NormalizedEntry>> = Vec::new();
        let mut sessions = Vec::new();
        for msg in store.get_history() {
            match msg {
                LogMsg::JsonPatch(patch) => {
                    if let Some((index, entry)) = extract_normalized_entry_from_patch(&patch) {
                        if entries.len() <= index {
                            entries.resize(index + 1, None);
                        }
                        entries[index] = Some(entry);
                    }
                }
                LogMsg::SessionId(id) => sessions.push(id),
                _ => {}
            }
        }
        (entries.into_iter().flatten().collect(), sessions)
    }

    fn fixture_lines() -> Vec<&'static str> {
        FIXTURE_0_169
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect()
    }

    #[test]
    fn fixture_pushes_thread_ids_as_session_ids() {
        let (_, sessions) = run(&fixture_lines());
        assert_eq!(
            sessions,
            vec![
                "01a1e9f9-cd5c-7433-9217-757e071bd3c1".to_string(),
                "01a1e9fa-f857-7ff2-b539-63a832475f55".to_string(),
            ]
        );
    }

    #[test]
    fn fixture_streams_and_finalizes_assistant_messages() {
        let (entries, _) = run(&fixture_lines());
        let messages: Vec<&str> = entries
            .iter()
            .filter(|e| matches!(e.entry_type, NormalizedEntryType::AssistantMessage))
            .map(|e| e.content.as_str())
            .collect();
        assert_eq!(messages.len(), 4);
        assert!(messages[0].starts_with("Plan:"));
        assert!(messages[2].starts_with("Fixed `calc.py`"));
    }

    #[test]
    fn fixture_renders_commands_edits_and_tools() {
        let (entries, _) = run(&fixture_lines());
        let commands: Vec<(String, ToolStatus)> = entries
            .iter()
            .filter_map(|e| match &e.entry_type {
                NormalizedEntryType::ToolUse {
                    action_type: ActionType::CommandRun { command, .. },
                    status,
                    ..
                } => Some((command.clone(), status.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].0, "Get-Content -LiteralPath 'calc.py'");
        assert!(matches!(commands[0].1, ToolStatus::Failed));

        let edit = entries
            .iter()
            .find_map(|e| match &e.entry_type {
                NormalizedEntryType::ToolUse {
                    action_type: ActionType::FileEdit { path, changes },
                    status,
                    ..
                } => Some((path.clone(), changes.len(), status.clone())),
                _ => None,
            })
            .expect("file edit entry");
        // `make_path_relative` is platform-aware; `/repo` is only absolute on Unix.
        assert_eq!(edit.0, make_path_relative("/repo/calc.py", "/repo"));
        assert!(edit.0.ends_with("calc.py"));
        assert_eq!(edit.1, 1);
        assert!(matches!(edit.2, ToolStatus::Success));

        assert!(entries.iter().any(|e| matches!(
            &e.entry_type,
            NormalizedEntryType::ToolUse { tool_name, status: ToolStatus::Failed, .. }
                if tool_name == "mcp:node_repl:js"
        )));
        assert!(
            entries
                .iter()
                .any(|e| matches!(e.entry_type, NormalizedEntryType::TokenUsageInfo(_)))
        );
    }

    #[test]
    fn file_change_entry_carries_approval_item_id() {
        let (entries, _) = run(&fixture_lines());
        let has_call_id = entries.iter().any(|e| {
            e.metadata
                .as_ref()
                .and_then(|m| m.get("tool_call_id"))
                .and_then(Value::as_str)
                == Some("exec-cbc0fd12-deca-437d-af74-36673b3544df")
        });
        assert!(has_call_id);
    }

    #[test]
    fn plan_update_becomes_todo_list() {
        let line = r#"{"method":"turn/plan/updated","params":{"threadId":"t","turnId":"u","explanation":null,"plan":[{"step":"a","status":"completed"},{"step":"b","status":"inProgress"}]}}"#;
        let (entries, _) = run(&[line]);
        match &entries[0].entry_type {
            NormalizedEntryType::ToolUse {
                action_type: ActionType::TodoManagement { todos, .. },
                ..
            } => {
                assert_eq!(todos.len(), 2);
                assert_eq!(todos[1].status, "in_progress");
            }
            other => panic!("unexpected entry {other:?}"),
        }
    }

    #[test]
    fn retryable_error_is_system_message() {
        let retry = r#"{"method":"error","params":{"error":{"message":"overloaded"},"willRetry":true,"threadId":"t","turnId":"u"}}"#;
        let fatal = r#"{"method":"error","params":{"error":{"message":"boom"},"willRetry":false,"threadId":"t","turnId":"u"}}"#;
        let (entries, _) = run(&[retry, fatal]);
        assert!(matches!(
            entries[0].entry_type,
            NormalizedEntryType::SystemMessage
        ));
        assert!(matches!(
            entries[1].entry_type,
            NormalizedEntryType::ErrorMessage { .. }
        ));
    }

    #[test]
    fn static_reply_line_renders_as_assistant_message() {
        let line = r#"{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"vk-static-0","text":"hello"}}}"#;
        let (entries, _) = run(&[line]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content, "hello");
    }

    #[test]
    fn display_command_prefers_parsed_actions() {
        let actions = vec![CommandAction {
            kind: "read".into(),
            command: Some("cat a.txt".into()),
        }];
        assert_eq!(
            display_command("pwsh -Command \"cat a.txt\"", &actions),
            "cat a.txt"
        );
        assert_eq!(display_command("ls", &[]), "ls");
    }

    #[test]
    fn denied_approval_becomes_user_feedback() {
        let line = Approval::approval_response(
            "x".into(),
            "codex.exec_command".into(),
            ApprovalStatus::Denied {
                reason: Some("no".into()),
            },
        )
        .raw();
        let (entries, _) = run(&[&line]);
        assert!(matches!(
            &entries[0].entry_type,
            NormalizedEntryType::UserFeedback { denied_tool } if denied_tool == "Exec Command"
        ));
    }
}
