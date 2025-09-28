use anyhow::Result;

use crate::utils::get_var;

#[derive(Debug, Clone)]
pub struct QueueSettings {
    pub max_message_count: usize,
    pub retry_count: usize,
    pub retry_interval_ms: u64,
    pub message_retention_ms: u64,
    pub wait_time_seconds: i32,
    pub visibility_timeout_secs: u32,
}

impl QueueSettings {
    pub fn new(
        max_message_count: usize,
        retry_count: usize,
        retry_interval_ms: u64,
        message_retention_ms: u64,
        wait_time_seconds: i32,
        visibility_timeout_secs: u32,
    ) -> Self {
        Self {
            max_message_count,
            retry_count,
            retry_interval_ms,
            message_retention_ms,
            wait_time_seconds,
            visibility_timeout_secs,
        }
    }

    pub fn from_env() -> Result<Self> {
        let queue_message_retention_ms: u64 =
            get_var("QUEUE_MESSAGE_RETENTION_PERIOD_MS")?.parse()?;
        let queue_retry_interval_ms: u64 = get_var("QUEUE_RETRY_INTERVAL_MS")?.parse()?;
        let queue_max_message_count: usize = get_var("QUEUE_MAX_MESSAGE_COUNT")?.parse()?;
        let queue_retry_count: usize = get_var("QUEUE_RETRY_COUNT")?.parse()?;
        let queue_wait_time_seconds: i32 = get_var("QUEUE_WAIT_TIME_SECONDS")?.parse()?;
        let queue_visibility_timeout_secs: u32 =
            get_var("QUEUE_VISIBILITY_TIMEOUT_SECS")?.parse()?;

        Ok(Self::new(
            queue_max_message_count,
            queue_retry_count,
            queue_retry_interval_ms,
            queue_message_retention_ms,
            queue_wait_time_seconds,
            queue_visibility_timeout_secs,
        ))
    }
}
