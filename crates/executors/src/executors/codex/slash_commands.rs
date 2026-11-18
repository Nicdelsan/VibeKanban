use std::{
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::{Value, json};

use super::{
    Codex,
    client::{AppServerClient, LogWriter},
};
use crate::{
    env::ExecutionEnv,
    executors::{
        ExecutorError, ExecutorExitResult, SpawnedChild,
        utils::{SlashCommandCall, parse_slash_command},
    },
    stdout_dup::spawn_local_output_process,
};

const CODEX_INIT_PROMPT: &str = include_str!("init_prompt.md");
const DEFAULT_PROJECT_DOC_FILENAME: &str = "AGENTS.md";

static STATIC_ITEM_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub enum CodexSlashCommand {
    Init,
    Compact { instructions: Option<String> },
    Status,
    Mcp,
}

impl CodexSlashCommand {
    pub fn parse(prompt: &str) -> Option<Self> {
        let cmd: SlashCommandCall<'_> = parse_slash_command(prompt)?;
        match cmd.name.as_str() {
            "init" => Some(Self::Init),
            "compact" => Some(Self::Compact {
                instructions: if cmd.arguments.is_empty() {
                    None
                } else {
                    Some(cmd.arguments.to_string())
                },
            }),
            "status" => Some(Self::Status),
            "mcp" => Some(Self::Mcp),
            _ => None,
        }
    }
}

impl Codex {
    pub async fn spawn_slash_command(
        &self,
        current_dir: &Path,
        prompt: &str,
        session_id: Option<&str>,
        env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        let Some(command) = CodexSlashCommand::parse(prompt) else {
            return self
                .spawn_agent_with_prompt(current_dir, prompt, session_id, env)
                .await;
        };
        match command {
            CodexSlashCommand::Init => {
                if current_dir.join(DEFAULT_PROJECT_DOC_FILENAME).exists() {
                    let message = format!(
                        "`{DEFAULT_PROJECT_DOC_FILENAME}` already exists. Skipping `/init` to avoid overwriting it."
                    );
                    self.return_static_reply(Ok(message)).await
                } else {
                    self.spawn_agent_with_prompt(current_dir, CODEX_INIT_PROMPT, session_id, env)
                        .await
                }
            }
            CodexSlashCommand::Compact { instructions } => match session_id {
                Some(session_id) => {
                    self.perform_compact(current_dir, session_id, instructions, env)
                        .await
                }
                None => {
                    self.return_static_reply(Ok("_No active session to compact._".to_string()))
                        .await
                }
            },
            CodexSlashCommand::Status => {
                self.return_static_reply(Ok(self.build_status_message(session_id)))
                    .await
            }
            CodexSlashCommand::Mcp => self.perform_mcp_status(current_dir, env).await,
        }
    }

    async fn spawn_agent_with_prompt(
        &self,
        current_dir: &Path,
        prompt: &str,
        session_id: Option<&str>,
        env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        let command_parts = match session_id {
            Some(_) => self.build_command_builder()?.build_follow_up(&[])?,
            None => self.build_command_builder()?.build_initial()?,
        };
        let combined_prompt = self.append_prompt.combine_prompt(prompt);
        let action = super::CodexSessionAction::Chat {
            prompt: combined_prompt,
        };
        self.spawn_inner(current_dir, command_parts, action, session_id, env)
            .await
    }

    /// Compact a forked copy of the session; the fork becomes the new session id.
    async fn perform_compact(
        &self,
        current_dir: &Path,
        session_id: &str,
        instructions: Option<String>,
        env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        let command_parts = self.build_command_builder()?.build_follow_up(&[])?;
        let mut settings = self.build_thread_settings(current_dir);
        if let Some(instructions) = instructions {
            settings
                .config
                .get_or_insert_with(serde_json::Map::new)
                .insert("compact_prompt".to_string(), Value::String(instructions));
        }
        let session_id = session_id.to_string();

        self.spawn_app_server(
            current_dir,
            command_parts,
            env,
            move |client, _| async move {
                client.ensure_authenticated().await?;
                let thread_id = Codex::open_thread(&client, &settings, Some(&session_id)).await?;
                client.compact_thread(&thread_id).await?;
                // Completion is signalled by the `turn/completed` notification.
                Ok(())
            },
        )
        .await
    }

    async fn perform_mcp_status(
        &self,
        current_dir: &Path,
        env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        let command_parts = self.build_command_builder()?.build_initial()?;
        self.spawn_app_server(
            current_dir,
            command_parts,
            env,
            move |client, exit_signal_tx| async move {
                let message = fetch_mcp_status_message(&client).await?;
                log_agent_message(client.log_writer(), &message).await?;
                // The app server emits no turn events for plain API calls.
                exit_signal_tx
                    .send_exit_signal(ExecutorExitResult::Success)
                    .await;
                Ok(())
            },
        )
        .await
    }

    pub async fn return_static_reply(
        &self,
        message: Result<String, String>,
    ) -> Result<SpawnedChild, ExecutorError> {
        let (mut spawned, writer) = spawn_local_output_process()?;
        let log_writer = LogWriter::new(writer);
        let (exit_signal_tx, exit_signal_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let result = match &message {
                Ok(text) => log_agent_message(&log_writer, text).await,
                Err(text) => log_error(&log_writer, text).await,
            };
            let exit_result = match result {
                Ok(()) => ExecutorExitResult::Success,
                Err(err) => {
                    tracing::error!("Failed to emit slash command output: {err}");
                    ExecutorExitResult::Failure
                }
            };
            let _ = exit_signal_tx.send(exit_result);
        });

        spawned.exit_signal = Some(exit_signal_rx);
        Ok(spawned)
    }

    fn build_status_message(&self, session_id: Option<&str>) -> String {
        let mut lines = vec![
            "# Session Status\n".to_string(),
            "## Configuration".to_string(),
        ];
        lines.push(format!(
            "- **Model**: `{}`",
            self.model.as_deref().unwrap_or("default")
        ));
        if let Some(policy) = self.approval_policy() {
            lines.push(format!("- **Approvals**: `{policy}`"));
        }
        lines.push(format!("- **Sandbox**: `{}`", self.sandbox_mode()));
        if let Some(effort) = &self.model_reasoning_effort {
            lines.push(format!("- **Reasoning effort**: `{}`", effort.as_ref()));
        }
        if let Some(session_id) = session_id {
            lines.push(format!("- **Session**: `{session_id}`"));
        }
        lines.push("\n_Token usage is reported in the conversation after each turn._".to_string());
        lines.join("\n")
    }
}

/// Emit a synthetic v2 `item/completed` agent message so the normalizer renders it.
pub async fn log_agent_message(log_writer: &LogWriter, text: &str) -> Result<(), ExecutorError> {
    let id = STATIC_ITEM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let notification = json!({
        "method": "item/completed",
        "params": { "item": { "type": "agentMessage", "id": format!("vk-static-{id}"), "text": text } },
    });
    log_writer.log_raw(&notification.to_string()).await
}

async fn log_error(log_writer: &LogWriter, message: &str) -> Result<(), ExecutorError> {
    let notification = json!({
        "method": "error",
        "params": { "error": { "message": message }, "willRetry": false },
    });
    log_writer.log_raw(&notification.to_string()).await
}

async fn fetch_mcp_status_message(client: &AppServerClient) -> Result<String, ExecutorError> {
    let mut cursor = None;
    let mut servers = Vec::new();
    loop {
        let response = client.list_mcp_server_status(cursor).await?;
        if let Some(data) = response.get("data").and_then(Value::as_array) {
            servers.extend(data.iter().cloned());
        }
        cursor = response
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    Ok(format_mcp_status(&servers))
}

fn sorted_names(values: Option<&Value>) -> Vec<String> {
    let mut names: Vec<String> = values
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("name").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn format_mcp_status(servers: &[Value]) -> String {
    if servers.is_empty() {
        return "_No MCP servers configured._".to_string();
    }
    let mut lines = vec![format!("# MCP Servers ({})\n", servers.len())];
    for server in servers {
        let name = server.get("name").and_then(Value::as_str).unwrap_or("?");
        let auth = server
            .get("authStatus")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        lines.push(format!("## {name}"));
        lines.push(format!("- **Auth**: `{auth}`"));

        let mut tools: Vec<String> = server
            .get("tools")
            .and_then(Value::as_object)
            .map(|tools| tools.keys().cloned().collect())
            .unwrap_or_default();
        tools.sort();
        if tools.is_empty() {
            lines.push("- **Tools**: _none_".to_string());
        } else {
            lines.push(format!("- **Tools**: `{}`", tools.join("`, `")));
        }

        let resources = sorted_names(server.get("resources"));
        if !resources.is_empty() {
            lines.push(format!("- **Resources**: `{}`", resources.join("`, `")));
        }
        let templates = sorted_names(server.get("resourceTemplates"));
        if !templates.is_empty() {
            lines.push(format!(
                "- **Resource Templates**: `{}`",
                templates.join("`, `")
            ));
        }
        lines.push(String::new());
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_slash_commands() {
        assert!(matches!(
            CodexSlashCommand::parse("/init"),
            Some(CodexSlashCommand::Init)
        ));
        assert!(matches!(
            CodexSlashCommand::parse("/compact keep tests"),
            Some(CodexSlashCommand::Compact {
                instructions: Some(_)
            })
        ));
        assert!(CodexSlashCommand::parse("hello").is_none());
    }

    #[test]
    fn formats_mcp_status_from_v2_payload() {
        let servers = vec![json!({
            "name": "docs",
            "authStatus": "oAuth",
            "tools": { "search": {}, "fetch": {} },
            "resources": [{ "name": "b" }, { "name": "a" }],
            "resourceTemplates": [],
        })];
        let text = format_mcp_status(&servers);
        assert!(text.contains("## docs"));
        assert!(text.contains("`fetch`, `search`"));
        assert!(text.contains("`a`, `b`"));
    }

    #[test]
    fn empty_mcp_status_message() {
        assert_eq!(format_mcp_status(&[]), "_No MCP servers configured._");
    }
}
