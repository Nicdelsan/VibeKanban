use std::{
    borrow::Cow,
    collections::VecDeque,
    io,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use workspace_utils::approvals::ApprovalStatus;

use super::{
    jsonrpc::{JsonRpcCallbacks, JsonRpcPeer},
    protocol::{
        AccountResponse, ApprovalDecision, ApprovalRequestParams, JsonRpcError, JsonRpcErrorBody,
        JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, METHOD_COMMAND_APPROVAL,
        METHOD_FILE_CHANGE_APPROVAL, RequestId, ReviewTarget, ThreadResponse, ThreadSettings,
    },
};
use crate::{
    approvals::{ExecutorApprovalError, ExecutorApprovalService},
    env::RepoContext,
    executors::{ExecutorError, codex::normalize_logs::Approval},
};

/// JSON-RPC "method not found" error code.
const METHOD_NOT_FOUND: i64 = -32601;

pub struct AppServerClient {
    rpc: OnceLock<JsonRpcPeer>,
    log_writer: LogWriter,
    approvals: Option<Arc<dyn ExecutorApprovalService>>,
    thread_id: Mutex<Option<String>>,
    pending_feedback: Mutex<VecDeque<String>>,
    auto_approve: bool,
    repo_context: RepoContext,
    commit_reminder: bool,
    commit_reminder_sent: AtomicBool,
    cancel: CancellationToken,
}

impl AppServerClient {
    pub fn new(
        log_writer: LogWriter,
        approvals: Option<Arc<dyn ExecutorApprovalService>>,
        auto_approve: bool,
        repo_context: RepoContext,
        commit_reminder: bool,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            rpc: OnceLock::new(),
            log_writer,
            approvals,
            auto_approve,
            thread_id: Mutex::new(None),
            pending_feedback: Mutex::new(VecDeque::new()),
            repo_context,
            commit_reminder,
            commit_reminder_sent: AtomicBool::new(false),
            cancel,
        })
    }

    pub fn connect(&self, peer: JsonRpcPeer) {
        let _ = self.rpc.set(peer);
    }

    fn rpc(&self) -> &JsonRpcPeer {
        self.rpc.get().expect("Codex RPC peer not attached")
    }

    pub fn log_writer(&self) -> &LogWriter {
        &self.log_writer
    }

    pub async fn initialize(&self) -> Result<(), ExecutorError> {
        let params = json!({
            "clientInfo": {
                "name": "vibe-codex-executor",
                "title": null,
                "version": env!("CARGO_PKG_VERSION"),
            }
        });
        self.send_request::<Value>("initialize", params).await?;
        self.rpc()
            .send(&JsonRpcNotification {
                method: "initialized".to_string(),
                params: None,
            })
            .await
    }

    /// Fail early with a setup error when Codex is not logged in.
    pub async fn ensure_authenticated(&self) -> Result<(), ExecutorError> {
        let account: AccountResponse = self
            .send_request("account/read", json!({ "refreshToken": false }))
            .await?;
        if account.requires_openai_auth && account.account.is_none() {
            return Err(ExecutorError::AuthRequired(
                "Codex authentication required".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn start_thread(
        &self,
        settings: &ThreadSettings,
    ) -> Result<ThreadResponse, ExecutorError> {
        self.send_request("thread/start", to_params(settings)?)
            .await
    }

    /// Fork an existing thread so follow-ups never mutate the original session.
    pub async fn fork_thread(
        &self,
        thread_id: &str,
        settings: &ThreadSettings,
    ) -> Result<ThreadResponse, ExecutorError> {
        let params = with_thread_id(to_params(settings)?, thread_id);
        self.send_request("thread/fork", params).await
    }

    pub async fn resume_thread(
        &self,
        thread_id: &str,
        settings: &ThreadSettings,
    ) -> Result<ThreadResponse, ExecutorError> {
        let params = with_thread_id(to_params(settings)?, thread_id);
        self.send_request("thread/resume", params).await
    }

    pub async fn start_turn(&self, thread_id: &str, text: String) -> Result<Value, ExecutorError> {
        self.send_request("turn/start", turn_start_params(thread_id, text))
            .await
    }

    pub async fn start_review(
        &self,
        thread_id: &str,
        target: ReviewTarget,
    ) -> Result<Value, ExecutorError> {
        let params = json!({ "threadId": thread_id, "target": target });
        self.send_request("review/start", params).await
    }

    pub async fn compact_thread(&self, thread_id: &str) -> Result<Value, ExecutorError> {
        self.send_request("thread/compact/start", json!({ "threadId": thread_id }))
            .await
    }

    pub async fn list_mcp_server_status(
        &self,
        cursor: Option<String>,
    ) -> Result<Value, ExecutorError> {
        self.send_request(
            "mcpServerStatus/list",
            json!({ "cursor": cursor, "limit": null }),
        )
        .await
    }

    pub async fn register_session(&self, thread_id: &str) -> Result<(), ExecutorError> {
        self.thread_id.lock().await.replace(thread_id.to_string());
        Ok(())
    }

    async fn handle_server_request(
        &self,
        peer: &JsonRpcPeer,
        request: JsonRpcRequest,
    ) -> Result<(), ExecutorError> {
        let (tool_name, log_tool_name) = match request.method.as_str() {
            METHOD_COMMAND_APPROVAL => ("bash", "codex.exec_command"),
            METHOD_FILE_CHANGE_APPROVAL => ("edit", "codex.apply_patch"),
            "item/tool/requestUserInput" => {
                // Vibe Kanban has no UI for free-form questions; answer with nothing.
                return send_result(peer, request.id, json!({ "answers": {} })).await;
            }
            other => {
                tracing::debug!("Unsupported Codex server request `{other}`");
                return send_error(peer, request.id, format!("unsupported request: {other}")).await;
            }
        };

        let raw_params = request.params.clone().unwrap_or(Value::Null);
        let params: ApprovalRequestParams = serde_json::from_value(raw_params.clone())
            .map_err(|err| ExecutorError::Io(io::Error::other(err.to_string())))?;
        let status = self
            .request_tool_approval(tool_name, raw_params, &params.item_id)
            .await
            .inspect_err(|err| {
                if !matches!(
                    err,
                    ExecutorError::ExecutorApprovalError(ExecutorApprovalError::Cancelled)
                ) {
                    tracing::error!(
                        "Codex {tool_name} approval failed for item {}: {err}",
                        params.item_id
                    );
                }
            })?;
        self.log_writer
            .log_raw(
                &Approval::approval_response(
                    params.item_id.clone(),
                    log_tool_name.to_string(),
                    status.clone(),
                )
                .raw(),
            )
            .await?;

        let (decision, feedback) = self.review_decision(&status);
        send_result(peer, request.id, json!({ "decision": decision })).await?;
        if let Some(message) = feedback {
            tracing::debug!("queueing {tool_name} denial feedback: {message}");
            self.enqueue_feedback(message).await;
        }
        Ok(())
    }

    async fn request_tool_approval(
        &self,
        tool_name: &str,
        tool_input: Value,
        tool_call_id: &str,
    ) -> Result<ApprovalStatus, ExecutorError> {
        if self.auto_approve {
            return Ok(ApprovalStatus::Approved);
        }
        let approval_service = self
            .approvals
            .as_ref()
            .ok_or(ExecutorApprovalError::ServiceUnavailable)?;

        Ok(approval_service
            .request_tool_approval(tool_name, tool_input, tool_call_id, self.cancel.clone())
            .await?)
    }

    async fn send_request<R>(&self, method: &str, params: Value) -> Result<R, ExecutorError>
    where
        R: DeserializeOwned + std::fmt::Debug,
    {
        let request = JsonRpcRequest {
            id: self.rpc().next_request_id(),
            method: method.to_string(),
            params: Some(params),
        };
        self.rpc()
            .request(request.id.clone(), &request, method, self.cancel.clone())
            .await
    }

    fn review_decision(&self, status: &ApprovalStatus) -> (ApprovalDecision, Option<String>) {
        if self.auto_approve {
            return (ApprovalDecision::AcceptForSession, None);
        }
        match status {
            ApprovalStatus::Approved => (ApprovalDecision::Accept, None),
            ApprovalStatus::Denied { reason } => {
                let feedback = reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                match feedback {
                    // Stop the turn so the feedback can be sent as the next user message.
                    Some(feedback) => (ApprovalDecision::Cancel, Some(feedback)),
                    None => (ApprovalDecision::Decline, None),
                }
            }
            ApprovalStatus::TimedOut | ApprovalStatus::Pending => (ApprovalDecision::Decline, None),
        }
    }

    async fn enqueue_feedback(&self, message: String) {
        if message.trim().is_empty() {
            return;
        }
        self.pending_feedback.lock().await.push_back(message);
    }

    /// Send queued denial feedback as a new turn. Returns true when a turn was started.
    async fn flush_pending_feedback(&self) -> bool {
        let messages: Vec<String> = self.pending_feedback.lock().await.drain(..).collect();
        if messages.is_empty() {
            return false;
        }
        let Some(thread_id) = self.thread_id.lock().await.clone() else {
            tracing::warn!(
                "pending Codex feedback but thread id unavailable; dropping {} messages",
                messages.len()
            );
            return false;
        };
        let text = messages
            .iter()
            .map(|m| format!("User feedback: {}", m.trim()))
            .collect::<Vec<_>>()
            .join("\n\n");
        self.spawn_turn(thread_id, text);
        true
    }

    fn spawn_turn(&self, thread_id: String, text: String) {
        let peer = self.rpc().clone();
        let cancel = self.cancel.clone();
        let request = JsonRpcRequest {
            id: peer.next_request_id(),
            method: "turn/start".to_string(),
            params: Some(turn_start_params(&thread_id, text)),
        };
        tokio::spawn(async move {
            if let Err(err) = peer
                .request::<Value, _>(request.id.clone(), &request, "turn/start", cancel)
                .await
            {
                tracing::error!("failed to start Codex follow-up turn: {err}");
            }
        });
    }

    /// Decide what to do when a turn completes. Returns true when the session is finished.
    async fn on_turn_completed(&self) -> bool {
        if self.flush_pending_feedback().await {
            return false;
        }
        if self.commit_reminder && !self.commit_reminder_sent.swap(true, Ordering::SeqCst) {
            let status = self.repo_context.check_uncommitted_changes().await;
            if !status.is_empty()
                && let Some(thread_id) = self.thread_id.lock().await.clone()
            {
                self.spawn_turn(
                    thread_id,
                    format!(
                        "You have uncommitted changes. Please stage and commit them now with a descriptive commit message.{status}"
                    ),
                );
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl JsonRpcCallbacks for AppServerClient {
    async fn on_request(
        &self,
        peer: &JsonRpcPeer,
        raw: &str,
        request: JsonRpcRequest,
    ) -> Result<(), ExecutorError> {
        self.log_writer.log_raw(raw).await?;
        self.handle_server_request(peer, request).await
    }

    async fn on_response(
        &self,
        _peer: &JsonRpcPeer,
        raw: &str,
        response: &JsonRpcResponse,
    ) -> Result<(), ExecutorError> {
        // Only thread/turn responses matter to the log normalizer. Others (e.g. account/read)
        // may contain account details and are not persisted.
        let Some(result) = response.result.as_object() else {
            return Ok(());
        };
        if !(result.contains_key("thread") || result.contains_key("turn")) {
            return Ok(());
        }
        let line = strip_thread_turns(raw);
        self.log_writer.log_raw(&line).await
    }

    async fn on_error(
        &self,
        _peer: &JsonRpcPeer,
        raw: &str,
        _error: &JsonRpcError,
    ) -> Result<(), ExecutorError> {
        self.log_writer.log_raw(raw).await
    }

    async fn on_notification(
        &self,
        _peer: &JsonRpcPeer,
        raw: &str,
        notification: JsonRpcNotification,
    ) -> Result<bool, ExecutorError> {
        if is_private_notification(&notification.method) {
            return Ok(false);
        }
        let line = if notification.method == "thread/started" {
            strip_thread_turns(raw)
        } else {
            Cow::Borrowed(raw)
        };
        self.log_writer.log_raw(&line).await?;

        if notification.method == "turn/completed" {
            return Ok(self.on_turn_completed().await);
        }
        Ok(false)
    }

    async fn on_non_json(&self, raw: &str) -> Result<(), ExecutorError> {
        self.log_writer.log_raw(raw).await?;
        Ok(())
    }
}

/// Notifications carrying account or machine details (plan, rate limits, host name). The
/// normalizer ignores them, so they are never persisted.
fn is_private_notification(method: &str) -> bool {
    method.starts_with("account/") || method.starts_with("remoteControl/")
}

fn to_params<T: Serialize>(value: &T) -> Result<Value, ExecutorError> {
    serde_json::to_value(value).map_err(|err| ExecutorError::Io(io::Error::other(err.to_string())))
}

/// Add the target thread id and skip history hydration (deprecated, and can be huge).
fn with_thread_id(mut params: Value, thread_id: &str) -> Value {
    if let Some(obj) = params.as_object_mut() {
        obj.insert("threadId".to_string(), Value::String(thread_id.to_string()));
        obj.insert("excludeTurns".to_string(), Value::Bool(true));
    }
    params
}

fn turn_start_params(thread_id: &str, text: String) -> Value {
    json!({
        "threadId": thread_id,
        "input": [{ "type": "text", "text": text, "text_elements": [] }],
    })
}

/// Thread payloads can embed the whole conversation history; drop it so log lines stay small.
fn strip_thread_turns(raw: &str) -> Cow<'_, str> {
    let Ok(mut value) = serde_json::from_str::<Value>(raw) else {
        return Cow::Borrowed(raw);
    };
    let removed = ["/result/thread", "/params/thread"].iter().any(|pointer| {
        value
            .pointer_mut(pointer)
            .and_then(Value::as_object_mut)
            .is_some_and(|thread| thread.remove("turns").is_some())
    });
    if removed {
        Cow::Owned(serde_json::to_string(&value).unwrap_or_else(|_| raw.to_string()))
    } else {
        Cow::Borrowed(raw)
    }
}

async fn send_result(
    peer: &JsonRpcPeer,
    id: RequestId,
    result: Value,
) -> Result<(), ExecutorError> {
    peer.send(&JsonRpcResponse { id, result }).await
}

async fn send_error(
    peer: &JsonRpcPeer,
    id: RequestId,
    message: String,
) -> Result<(), ExecutorError> {
    peer.send(&JsonRpcError {
        id,
        error: JsonRpcErrorBody {
            code: METHOD_NOT_FOUND,
            message,
            data: None,
        },
    })
    .await
}

#[derive(Clone)]
pub struct LogWriter {
    writer: Arc<Mutex<BufWriter<Box<dyn AsyncWrite + Send + Unpin>>>>,
}

impl LogWriter {
    pub fn new(writer: impl AsyncWrite + Send + Unpin + 'static) -> Self {
        Self {
            writer: Arc::new(Mutex::new(BufWriter::new(Box::new(writer)))),
        }
    }

    pub async fn log_raw(&self, raw: &str) -> Result<(), ExecutorError> {
        let mut guard = self.writer.lock().await;
        guard
            .write_all(raw.as_bytes())
            .await
            .map_err(ExecutorError::Io)?;
        guard.write_all(b"\n").await.map_err(ExecutorError::Io)?;
        guard.flush().await.map_err(ExecutorError::Io)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_turns_from_thread_response() {
        let raw = r#"{"id":3,"result":{"thread":{"id":"t","turns":[{"id":"x"}]},"model":"m"}}"#;
        let stripped: Value = serde_json::from_str(&strip_thread_turns(raw)).unwrap();
        assert!(stripped.pointer("/result/thread/turns").is_none());
        assert_eq!(stripped.pointer("/result/thread/id"), Some(&json!("t")));
    }

    #[test]
    fn strips_turns_from_thread_started_notification() {
        let raw = r#"{"method":"thread/started","params":{"thread":{"id":"t","turns":[]}}}"#;
        let stripped: Value = serde_json::from_str(&strip_thread_turns(raw)).unwrap();
        assert!(stripped.pointer("/params/thread/turns").is_none());
    }

    #[test]
    fn account_and_remote_control_notifications_are_private() {
        assert!(is_private_notification("account/updated"));
        assert!(is_private_notification("account/rateLimits/updated"));
        assert!(is_private_notification("remoteControl/status/changed"));
        assert!(!is_private_notification("turn/completed"));
    }

    #[test]
    fn thread_id_and_exclude_turns_are_added() {
        let params = with_thread_id(json!({"model": "m"}), "abc");
        assert_eq!(params["threadId"], json!("abc"));
        assert_eq!(params["excludeTurns"], json!(true));
        assert_eq!(params["model"], json!("m"));
    }
}
