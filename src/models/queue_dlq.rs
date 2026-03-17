use serde_json::json;

#[derive(Debug, Clone)]
pub struct QueueDlq {
    pub arn: String,
    pub max_receive_count: u32,
}

impl QueueDlq {
    pub fn new(arn: &str, max_receive_count: u32) -> Self {
        Self {
            arn: arn.to_string(),
            max_receive_count,
        }
    }

    pub fn queue_dlq_json(&self) -> String {
        json!({
            "deadLetterTargetArn": self.arn,
            "maxReceiveCount": self.max_receive_count.to_string(),
        })
        .to_string()
    }
}
