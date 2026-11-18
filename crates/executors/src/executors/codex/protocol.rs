//! Minimal, version-tolerant types for the Codex app-server v2 protocol.
//!
//! Only the subset used by the executor is modelled. Every struct ignores unknown fields and
//! enums carry catch-all variants, so newer Codex releases that add fields or item types keep
//! working. Shapes follow `codex app-server generate-json-schema` (Codex CLI 0.169).

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// JSON-RPC envelope
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Integer(i64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub id: RequestId,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub id: RequestId,
    pub result: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcErrorBody {
    #[serde(default)]
    pub code: i64,
    #[serde(default)]
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub id: RequestId,
    pub error: JsonRpcErrorBody,
}

/// Incoming message classified by which JSON-RPC fields are present.
#[derive(Debug, Clone)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Notification(JsonRpcNotification),
    Response(JsonRpcResponse),
    Error(JsonRpcError),
}

impl JsonRpcMessage {
    pub fn parse(line: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(line).ok()?;
        let obj = value.as_object()?;
        let has = |key: &str| obj.contains_key(key);
        if has("method") && has("id") {
            serde_json::from_value(value).ok().map(Self::Request)
        } else if has("method") {
            serde_json::from_value(value).ok().map(Self::Notification)
        } else if has("error") {
            serde_json::from_value(value).ok().map(Self::Error)
        } else if has("result") && has("id") {
            serde_json::from_value(value).ok().map(Self::Response)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Client → server
// ---------------------------------------------------------------------------

/// Thread-level settings shared by `thread/start`, `thread/resume` and `thread/fork`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReviewTarget {
    UncommittedChanges,
    BaseBranch { branch: String },
    Custom { instructions: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadRef {
    pub id: String,
}

/// Response of `thread/start`, `thread/resume` and `thread/fork`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadResponse {
    pub thread: ThreadRef,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountResponse {
    #[serde(default)]
    pub account: Option<Value>,
    #[serde(default)]
    pub requires_openai_auth: bool,
}

// ---------------------------------------------------------------------------
// Server → client notifications
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemNotification {
    pub item: ThreadItem,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemDeltaNotification {
    pub item_id: String,
    #[serde(default)]
    pub delta: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnCompletedNotification {
    pub turn: Turn,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub error: Option<TurnError>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnError {
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub additional_details: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorNotification {
    pub error: TurnError,
    #[serde(default)]
    pub will_retry: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageNotification {
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageNotification {
    pub token_usage: ThreadTokenUsage,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTokenUsage {
    pub last: TokenUsageBreakdown,
    #[serde(default)]
    pub model_context_window: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageBreakdown {
    #[serde(default)]
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanUpdatedNotification {
    #[serde(default)]
    pub explanation: Option<String>,
    #[serde(default)]
    pub plan: Vec<PlanStep>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanStep {
    pub step: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartedNotification {
    pub thread: ThreadRef,
}

// ---------------------------------------------------------------------------
// Server → client requests (approvals)
// ---------------------------------------------------------------------------

pub const METHOD_COMMAND_APPROVAL: &str = "item/commandExecution/requestApproval";
pub const METHOD_FILE_CHANGE_APPROVAL: &str = "item/fileChange/requestApproval";
pub const METHOD_PERMISSIONS_APPROVAL: &str = "item/permissions/requestApproval";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequestParams {
    pub item_id: String,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Decision values shared by command and file-change approvals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

// ---------------------------------------------------------------------------
// Thread items
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ThreadItem {
    #[serde(rename_all = "camelCase")]
    UserMessage { id: String },
    #[serde(rename_all = "camelCase")]
    AgentMessage {
        id: String,
        #[serde(default)]
        text: String,
    },
    #[serde(rename_all = "camelCase")]
    Plan {
        id: String,
        #[serde(default)]
        text: String,
    },
    #[serde(rename_all = "camelCase")]
    Reasoning {
        id: String,
        #[serde(default)]
        summary: Vec<String>,
        #[serde(default)]
        content: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    CommandExecution {
        id: String,
        #[serde(default)]
        command: String,
        #[serde(default)]
        command_actions: Vec<CommandAction>,
        #[serde(default)]
        aggregated_output: Option<String>,
        #[serde(default)]
        exit_code: Option<i32>,
        #[serde(default)]
        status: ItemStatus,
    },
    #[serde(rename_all = "camelCase")]
    FileChange {
        id: String,
        #[serde(default)]
        changes: Vec<FileUpdateChange>,
        #[serde(default)]
        status: ItemStatus,
    },
    #[serde(rename_all = "camelCase")]
    McpToolCall {
        id: String,
        #[serde(default)]
        server: String,
        #[serde(default)]
        tool: String,
        #[serde(default)]
        arguments: Value,
        #[serde(default)]
        result: Option<McpToolCallResult>,
        #[serde(default)]
        error: Option<Value>,
        #[serde(default)]
        status: ItemStatus,
    },
    #[serde(rename_all = "camelCase")]
    DynamicToolCall {
        id: String,
        #[serde(default)]
        tool: String,
        #[serde(default)]
        arguments: Value,
        #[serde(default)]
        success: Option<bool>,
        #[serde(default)]
        status: ItemStatus,
    },
    #[serde(rename_all = "camelCase")]
    WebSearch {
        id: String,
        #[serde(default)]
        query: String,
    },
    #[serde(rename_all = "camelCase")]
    ContextCompaction { id: String },
    #[serde(rename_all = "camelCase")]
    EnteredReviewMode {
        id: String,
        #[serde(default)]
        review: String,
    },
    #[serde(rename_all = "camelCase")]
    ExitedReviewMode {
        id: String,
        #[serde(default)]
        review: String,
    },
    /// Item types this executor does not render.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ItemStatus {
    #[default]
    InProgress,
    Completed,
    Failed,
    Declined,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandAction {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub command: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileUpdateChange {
    pub path: String,
    #[serde(default)]
    pub diff: String,
    pub kind: PatchChangeKind,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PatchChangeKind {
    Add,
    Delete,
    Update {
        #[serde(default)]
        move_path: Option<String>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolCallResult {
    #[serde(default)]
    pub content: Vec<Value>,
    #[serde(default)]
    pub structured_content: Option<Value>,
    #[serde(default)]
    pub is_error: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_jsonrpc_messages() {
        let req = r#"{"method":"item/fileChange/requestApproval","id":1,"params":{"itemId":"x"}}"#;
        assert!(matches!(
            JsonRpcMessage::parse(req),
            Some(JsonRpcMessage::Request(_))
        ));
        let note = r#"{"method":"turn/started","params":{}}"#;
        assert!(matches!(
            JsonRpcMessage::parse(note),
            Some(JsonRpcMessage::Notification(_))
        ));
        let resp = r#"{"id":2,"result":{"ok":true}}"#;
        assert!(matches!(
            JsonRpcMessage::parse(resp),
            Some(JsonRpcMessage::Response(_))
        ));
        let err = r#"{"id":"a","error":{"code":-1,"message":"boom"}}"#;
        assert!(matches!(
            JsonRpcMessage::parse(err),
            Some(JsonRpcMessage::Error(_))
        ));
        assert!(JsonRpcMessage::parse("not json").is_none());
    }

    #[test]
    fn unknown_thread_item_type_is_tolerated() {
        let item: ThreadItem =
            serde_json::from_str(r#"{"type":"imageGeneration","id":"1","status":"x"}"#).unwrap();
        assert!(matches!(item, ThreadItem::Unknown));
    }

    #[test]
    fn approval_decision_serializes_camel_case() {
        let json = serde_json::to_string(&ApprovalDecision::AcceptForSession).unwrap();
        assert_eq!(json, r#""acceptForSession""#);
    }

    #[test]
    fn review_target_serializes_tagged() {
        let json = serde_json::to_value(ReviewTarget::Custom {
            instructions: "x".into(),
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "custom", "instructions": "x"})
        );
    }
}
