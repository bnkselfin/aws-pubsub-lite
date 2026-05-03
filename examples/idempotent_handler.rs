//! Idempotent handler with `ProcessedAlready` signaling.
//!
//! Demonstrates the idempotency contract every `BaseMessageHandler` should
//! follow:
//! 1. Be idempotent — same input twice produces the same state.
//! 2. Return `HandlerError::ProcessedAlready` when a duplicate is detected.
//!
//! The library uses rule 2 to safely delete duplicates under
//! `MessageDeleteMode::DeleteAllHandled`. Without it, duplicates can't be
//! cleanly removed.
//!
//! Run with:
//! ```
//! cargo run --example idempotent_handler
//! ```
//!
//! The handler keeps a `Mutex<HashSet<String>>` of message IDs it has already
//! processed. The example publishes the same message twice (with the same
//! "id" field in the JSON) and shows the second receipt being recognized as
//! a duplicate.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use aws_pubsub_lite::{
    PubSub,
    errors::HandlerError,
    models::{
        BaseMessageHandler, HandlerExecutionMode, IncomingMessage, MessageDeleteMode, ResourceName,
        ResourceNamingOptions, ResourceType, SeparatorSymbol,
    },
    settings::{AwsSettings, QueueSettings},
    utils::new_sdk_config,
};
use tokio_util::sync::CancellationToken;

struct DedupHandler {
    seen: Mutex<HashSet<String>>,
}

impl DedupHandler {
    fn new() -> Self {
        Self {
            seen: Mutex::new(HashSet::new()),
        }
    }
}

impl BaseMessageHandler for DedupHandler {
    fn handler_name(&self) -> &'static str {
        "DedupHandler"
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
                    handler: "DedupHandler".to_string(),
                    source: Box::new(e),
                })?;
            let inner_str = envelope["Message"].as_str().unwrap_or("");
            let inner: serde_json::Value =
                serde_json::from_str(inner_str).map_err(|e| HandlerError::ProcessingMessage {
                    message: "inner payload not JSON".to_string(),
                    handler: "DedupHandler".to_string(),
                    source: Box::new(e),
                })?;

            // The dedup key is the caller-supplied "id" field on the inner payload.
            let id = inner["id"].as_str().unwrap_or("").to_string();
            if id.is_empty() {
                return Err(HandlerError::ProcessingMessage {
                    message: "missing 'id' field".to_string(),
                    handler: "DedupHandler".to_string(),
                    source: Box::new(std::io::Error::other("missing id")),
                });
            }

            // Check-and-insert under one lock — atomic from the handler's POV.
            let mut seen = self.seen.lock().expect("dedup mutex poisoned");
            if seen.contains(&id) {
                return Err(HandlerError::ProcessedAlready {
                    message: format!("id={id}"),
                    handler: "DedupHandler".to_string(),
                });
            }
            seen.insert(id.clone());
            drop(seen);

            println!("[OK] first-time processing: id={id}");
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
            Some("idempotent"),
            Some(SeparatorSymbol::Hyphen),
            Some(SeparatorSymbol::Underscore),
        )
    };

    let topic = pubsub
        .add_topic(ResourceName::new("topic", ResourceType::Topic, naming())?)
        .await?;
    let queue = pubsub
        .add_queue(
            ResourceName::new("queue", ResourceType::Queue, naming())?,
            topic.arn(),
            None,
        )
        .await?;

    // Publish the same id twice — second one should be skipped via ProcessedAlready.
    let payloads = [
        r#"{"id":"order-42","amount":100}"#,
        r#"{"id":"order-43","amount":200}"#,
        r#"{"id":"order-42","amount":100}"#, // duplicate of the first
    ];
    for p in &payloads {
        pubsub.publish(topic.name(), p).await?;
    }
    println!("Published 3 messages (2 unique, 1 duplicate)");

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(45)).await;
        cancel_clone.cancel();
    });

    let handlers: Vec<Arc<dyn BaseMessageHandler>> = vec![Arc::new(DedupHandler::new())];
    pubsub
        .subscribe(
            queue.name(),
            handlers,
            HandlerExecutionMode::Sequential,
            MessageDeleteMode::DeleteAllHandled,
            cancel,
        )
        .await?;

    println!("Subscribe loop ended. The duplicate message should have produced a WARN log.");
    Ok(())
}
