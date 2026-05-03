//! Full DLQ wiring.
//!
//! Demonstrates:
//! - Provisioning a DLQ alongside a main queue.
//! - Wiring `RedrivePolicy` via `add_queue(..., Some(QueueDlq::new(...)))`.
//! - Handler failures triggering AWS-driven DLQ routing after `maxReceiveCount`.
//! - `MessageDeleteMode::DeleteAllHandled` keeping failed messages in the queue
//!   so they can redeliver and eventually reach the DLQ.
//!
//! Run with:
//! ```
//! cargo run --example dlq_wired
//! ```
//!
//! After the example finishes, check the DLQ:
//! ```
//! aws sqs get-queue-attributes \
//!   --queue-url <DLQ-URL> \
//!   --attribute-names ApproximateNumberOfMessages
//! ```

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use aws_pubsub_lite::{
    PubSub,
    errors::HandlerError,
    models::{
        BaseMessageHandler, HandlerExecutionMode, IncomingMessage, MessageDeleteMode, QueueDlq,
        ResourceName, ResourceNamingOptions, ResourceType, SeparatorSymbol,
    },
    settings::{AwsSettings, QueueSettings},
    utils::new_sdk_config,
};
use tokio_util::sync::CancellationToken;

const MAX_RECEIVE_COUNT: u32 = 3;

struct FailingHandler;

impl BaseMessageHandler for FailingHandler {
    fn handler_name(&self) -> &'static str {
        "FailingHandler"
    }

    fn handle(
        &self,
        message: &IncomingMessage,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + '_>> {
        let body = message.body().to_string();
        Box::pin(async move {
            let envelope: serde_json::Value =
                serde_json::from_str(&body).map_err(|e| HandlerError::ProcessingMessage {
                    message: "malformed envelope".to_string(),
                    handler: "FailingHandler".to_string(),
                    source: Box::new(e),
                })?;
            let inner = envelope["Message"].as_str().unwrap_or("");

            if inner.contains("fail") {
                return Err(HandlerError::ProcessingMessage {
                    message: format!("intentionally failing on '{inner}'"),
                    handler: "FailingHandler".to_string(),
                    source: Box::new(std::io::Error::other("intentional failure")),
                });
            }

            println!("[OK] processed: {inner}");
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let aws_settings = AwsSettings::from_env()?;
    let queue_settings = QueueSettings::from_env()?;
    let aws_config = new_sdk_config(&aws_settings).await?;

    let pubsub = PubSub::new(Some(queue_settings), None, aws_config).await?;

    let naming = || {
        ResourceNamingOptions::new(
            Some("examples"),
            Some("dlq-wired"),
            Some(SeparatorSymbol::Hyphen),
            Some(SeparatorSymbol::Underscore),
        )
    };

    let topic = pubsub
        .add_topic(ResourceName::new("topic", ResourceType::Topic, naming())?)
        .await?;

    // DLQ must exist before the main queue's RedrivePolicy can reference it.
    let dlq = pubsub
        .add_dlq(ResourceName::new("dlq", ResourceType::Queue, naming())?)
        .await?;

    let queue = pubsub
        .add_queue(
            ResourceName::new("queue", ResourceType::Queue, naming())?,
            topic.arn(),
            Some(QueueDlq::new(dlq.arn(), MAX_RECEIVE_COUNT)),
        )
        .await?;

    println!("Topic ARN: {}", topic.arn());
    println!("Queue URL: {}", queue.url());
    println!("DLQ URL:   {}", dlq.url());

    for msg in &["hello-1", "hello-2", "fail-this", "hello-3", "fail-that"] {
        pubsub.publish(topic.name(), msg).await?;
    }
    println!("Published 5 messages (3 ok, 2 'fail-*')");

    // Run for ~90s so failed messages can cycle through visibility timeout
    // MAX_RECEIVE_COUNT times and reach the DLQ.
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(90)).await;
        cancel_clone.cancel();
    });

    let handlers: Vec<Arc<dyn BaseMessageHandler>> = vec![Arc::new(FailingHandler)];
    pubsub
        .subscribe(
            queue.name(),
            handlers,
            HandlerExecutionMode::Sequential,
            MessageDeleteMode::DeleteAllHandled,
            cancel,
        )
        .await?;

    println!("Subscribe loop ended. Inspect DLQ depth:");
    println!(
        "  aws sqs get-queue-attributes --queue-url {} --attribute-names ApproximateNumberOfMessages",
        dlq.url()
    );

    Ok(())
}
