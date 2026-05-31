use anyhow::Result;
use serde_json::json;

use crate::models::BackoffFunction;
use crate::utils::get_var;

#[derive(Debug, Clone)]
pub struct TopicSettings {
    pub min_delay_target_secs: u32,
    pub max_delay_target_secs: u32,
    pub num_retries: u32,
    pub num_max_delay_retries: u32,
    pub num_no_delay_retries: u32,
    pub num_min_delay_retries: u32,
    pub backoff_function: BackoffFunction,
}

impl TopicSettings {
    pub fn new(
        min_delay_target_secs: u32,
        max_delay_target_secs: u32,
        num_retries: u32,
        num_max_delay_retries: u32,
        num_no_delay_retries: u32,
        num_min_delay_retries: u32,
        backoff_function: BackoffFunction,
    ) -> Self {
        Self {
            min_delay_target_secs,
            max_delay_target_secs,
            num_retries,
            num_max_delay_retries,
            num_no_delay_retries,
            num_min_delay_retries,
            backoff_function,
        }
    }

    pub fn from_env() -> Result<Self> {
        let min_delay_target_secs: u32 = get_var("PUBSUB_TOPIC_MIN_DELAY_TARGET_SECS")?.parse()?;
        let max_delay_target_secs: u32 = get_var("PUBSUB_TOPIC_MAX_DELAY_TARGET_SECS")?.parse()?;
        let num_retries: u32 = get_var("PUBSUB_TOPIC_NUM_RETRIES")?.parse()?;
        let num_max_delay_retries: u32 = get_var("PUBSUB_TOPIC_NUM_MAX_DELAY_RETRIES")?.parse()?;
        let num_no_delay_retries: u32 = get_var("PUBSUB_TOPIC_NUM_NO_DELAY_RETRIES")?.parse()?;
        let num_min_delay_retries: u32 = get_var("PUBSUB_TOPIC_NUM_MIN_DELAY_RETRIES")?.parse()?;
        let backoff_function: BackoffFunction = get_var("PUBSUB_TOPIC_BACKOFF_FUNCTION")?.parse()?;

        Ok(Self::new(
            min_delay_target_secs,
            max_delay_target_secs,
            num_retries,
            num_max_delay_retries,
            num_no_delay_retries,
            num_min_delay_retries,
            backoff_function,
        ))
    }

    pub fn delivery_policy_json(&self) -> String {
        json!({
            "http": {
                "defaultHealthyRetryPolicy": {
                    "minDelayTarget": self.min_delay_target_secs,
                    "maxDelayTarget": self.max_delay_target_secs,
                    "numRetries": self.num_retries,
                    "numMaxDelayRetries": self.num_max_delay_retries,
                    "numMinDelayRetries": self.num_min_delay_retries,
                    "numNoDelayRetries": self.num_no_delay_retries,
                    "backoffFunction": self.backoff_function.as_str(),
                },
                "disableSubscriptionOverrides": false,
            }
        })
        .to_string()
    }
}
