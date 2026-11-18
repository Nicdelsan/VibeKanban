use std::sync::Arc;

use super::{
    Codex,
    client::AppServerClient,
    protocol::{ReviewTarget, ThreadSettings},
};
use crate::executors::ExecutorError;

pub async fn launch_codex_review(
    settings: ThreadSettings,
    resume_session: Option<String>,
    review_target: ReviewTarget,
    client: Arc<AppServerClient>,
) -> Result<(), ExecutorError> {
    client.ensure_authenticated().await?;
    let thread_id = Codex::open_thread(&client, &settings, resume_session.as_deref()).await?;
    client.start_review(&thread_id, review_target).await?;
    Ok(())
}
