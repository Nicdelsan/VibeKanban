use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use derivative::Derivative;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use workspace_utils::{msg_store::MsgStore, shell::resolve_executable_path_blocking};

use crate::{
    approvals::ExecutorApprovalService,
    command::{CmdOverrides, CommandBuildError, CommandBuilder, apply_overrides},
    env::ExecutionEnv,
    executors::{
        AppendPrompt, AvailabilityInfo, ExecutorError, SpawnedChild, StandardCodingAgentExecutor,
        acp::AcpAgentHarness,
    },
};

const BASE_COMMAND: &str = "agy";
const SESSION_NAMESPACE: &str = "antigravity_sessions";
/// `agy` ends its turn when a command goes to background; under ACP nothing wakes it up again.
const WAIT_FOR_COMMANDS: &str = concat!(
    "\n\nNever end your turn while a command you started is still running in the ",
    "background: wait for it to finish and check its output first."
);

/// Antigravity session mode, applied through ACP `session/set_mode`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, TS, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[ts(rename = "AntigravityMode")]
pub enum AntigravityMode {
    /// Ask before edits and commands
    Default,
    /// Approve file edits in the workspace without asking
    AcceptEdits,
    /// Approve edits and safe commands; ask before irreversible actions
    Auto,
    /// Plan carefully before executing a task
    Plan,
}

impl AntigravityMode {
    fn as_acp_mode_id(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::AcceptEdits => "accept-edits",
            Self::Auto => "auto",
            Self::Plan => "plan",
        }
    }
}

/// Google Antigravity CLI (`agy`), driven through its native ACP server (`agy acp`).
#[derive(Derivative, Clone, Serialize, Deserialize, TS, JsonSchema)]
#[derivative(Debug, PartialEq)]
pub struct Antigravity {
    #[serde(default)]
    pub append_prompt: AppendPrompt,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        title = "Model",
        description = "Model id as listed by `agy models` (e.g. gemini-4-argon-medium, claude-sonnet-5-5-medium)"
    )]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(title = "Mode", description = "Agent execution mode")]
    pub mode: Option<AntigravityMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        title = "Skip Permissions",
        description = "Auto-approve all tool permission requests (--dangerously-skip-permissions)"
    )]
    pub yolo: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        title = "Sandbox",
        description = "Run with terminal restrictions enabled (--sandbox)"
    )]
    pub sandbox: Option<bool>,
    #[serde(flatten)]
    pub cmd: CmdOverrides,
    #[serde(skip)]
    #[ts(skip)]
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    pub approvals: Option<Arc<dyn ExecutorApprovalService>>,
}

impl Antigravity {
    fn build_command_builder(&self) -> Result<CommandBuilder, CommandBuildError> {
        let mut builder = CommandBuilder::new(BASE_COMMAND).params(["acp"]);
        if self.yolo.unwrap_or(false) {
            builder = builder.extend_params(["--dangerously-skip-permissions"]);
        }
        if self.sandbox.unwrap_or(false) {
            builder = builder.extend_params(["--sandbox"]);
        }
        apply_overrides(builder, &self.cmd)
    }

    fn combined_prompt(&self, prompt: &str) -> String {
        self.append_prompt.combine_prompt(prompt) + WAIT_FOR_COMMANDS
    }

    fn harness(&self) -> AcpAgentHarness {
        let harness = AcpAgentHarness::with_session_namespace(SESSION_NAMESPACE);
        let harness = match &self.model {
            Some(model) => harness.with_model(model.clone()),
            None => harness,
        };
        match self.mode {
            Some(mode) => harness.with_mode(mode.as_acp_mode_id()),
            None => harness,
        }
    }

    fn approvals_for_run(&self) -> Option<Arc<dyn ExecutorApprovalService>> {
        if self.yolo.unwrap_or(false) {
            None
        } else {
            self.approvals.clone()
        }
    }
}

#[async_trait]
impl StandardCodingAgentExecutor for Antigravity {
    fn use_approvals(&mut self, approvals: Arc<dyn ExecutorApprovalService>) {
        self.approvals = Some(approvals);
    }

    async fn spawn(
        &self,
        current_dir: &Path,
        prompt: &str,
        env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        let command = self.build_command_builder()?.build_initial()?;
        let combined_prompt = self.combined_prompt(prompt);
        self.harness()
            .spawn_with_command(
                current_dir,
                combined_prompt,
                command,
                env,
                &self.cmd,
                self.approvals_for_run(),
            )
            .await
    }

    async fn spawn_follow_up(
        &self,
        current_dir: &Path,
        prompt: &str,
        session_id: &str,
        _reset_to_message_id: Option<&str>,
        env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        let command = self.build_command_builder()?.build_follow_up(&[])?;
        let combined_prompt = self.combined_prompt(prompt);
        self.harness()
            .spawn_follow_up_with_command(
                current_dir,
                combined_prompt,
                session_id,
                command,
                env,
                &self.cmd,
                self.approvals_for_run(),
            )
            .await
    }

    fn normalize_logs(&self, msg_store: Arc<MsgStore>, worktree_path: &Path) {
        crate::executors::acp::normalize_logs(msg_store, worktree_path);
    }

    fn default_mcp_config_path(&self) -> Option<std::path::PathBuf> {
        dirs::home_dir().map(|home| home.join(".gemini").join("config").join("mcp_config.json"))
    }

    fn get_availability_info(&self) -> AvailabilityInfo {
        if self.cmd.base_command_override.is_none()
            && resolve_executable_path_blocking(BASE_COMMAND).is_none()
        {
            return AvailabilityInfo::NotFound;
        }

        let installation_found = dirs::home_dir()
            .map(|home| {
                home.join(".gemini")
                    .join("antigravity-cli")
                    .join("installation_id")
                    .exists()
            })
            .unwrap_or(false);

        if installation_found {
            AvailabilityInfo::InstallationFound
        } else {
            AvailabilityInfo::NotFound
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(json: serde_json::Value) -> Antigravity {
        serde_json::from_value(json).expect("valid Antigravity config")
    }

    fn args(agent: &Antigravity) -> String {
        format!(
            "{:?}",
            agent
                .build_command_builder()
                .unwrap()
                .build_initial()
                .unwrap()
        )
    }

    #[test]
    fn default_command_runs_acp_server() {
        let rendered = args(&config(serde_json::json!({})));
        assert!(rendered.contains("agy"));
        assert!(rendered.contains("acp"));
        assert!(!rendered.contains("--dangerously-skip-permissions"));
        assert!(!rendered.contains("--sandbox"));
    }

    #[test]
    fn yolo_and_sandbox_add_flags() {
        let rendered = args(&config(
            serde_json::json!({ "yolo": true, "sandbox": true }),
        ));
        assert!(rendered.contains("--dangerously-skip-permissions"));
        assert!(rendered.contains("--sandbox"));
    }

    #[test]
    fn mode_serializes_as_acp_mode_id() {
        let agent = config(serde_json::json!({ "mode": "accept-edits" }));
        assert_eq!(agent.mode, Some(AntigravityMode::AcceptEdits));
        assert_eq!(
            AntigravityMode::AcceptEdits.as_acp_mode_id(),
            "accept-edits"
        );
        assert_eq!(AntigravityMode::Plan.as_acp_mode_id(), "plan");
    }

    #[test]
    fn yolo_disables_approval_service() {
        let agent = config(serde_json::json!({ "yolo": true }));
        assert!(agent.approvals_for_run().is_none());
    }

    /// Collect stdout lines until the executor signals exit.
    async fn drain(mut spawned: SpawnedChild) -> Vec<String> {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let stdout = spawned.child.inner().stdout.take().unwrap();
        let exit = spawned.exit_signal.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        let mut out = Vec::new();
        let read = async {
            while let Ok(Some(line)) = lines.next_line().await {
                out.push(line);
            }
        };
        tokio::select! { _ = read => {}, _ = exit => {} }
        let _ = spawned.child.kill().await;
        out
    }

    #[tokio::test]
    #[ignore = "spawns the real agy CLI and spends tokens"]
    async fn agy_live_follow_up_keeps_context() {
        let cwd = std::env::temp_dir().join("vk-agy-live");
        std::fs::create_dir_all(&cwd).unwrap();
        let agent = config(serde_json::json!({ "yolo": true }));
        let env = ExecutionEnv::new(crate::env::RepoContext::new(cwd.clone(), vec![]), false);

        let first = drain(
            agent
                .spawn(&cwd, "Remember the word MANGO. Reply OK.", &env)
                .await
                .unwrap(),
        )
        .await;
        let sid = first
            .iter()
            .find_map(|l| {
                l.split("\"SessionStart\":\"")
                    .nth(1)?
                    .split('"')
                    .next()
                    .map(String::from)
            })
            .unwrap();

        let next = agent
            .spawn_follow_up(
                &cwd,
                "Which word did I ask you to remember? Word only.",
                &sid,
                None,
                &env,
            )
            .await
            .unwrap();
        let out = drain(next).await.join(
            "
",
        );
        println!("{out}");
        assert!(out.contains("MANGO"), "context kept");
        assert!(!out.contains("RESUME CONTEXT"), "no pasted history");
        assert!(!out.contains("Reply OK"), "replay not re-emitted");
    }
}
