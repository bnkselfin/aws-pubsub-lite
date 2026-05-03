//! Operator-style DLQ inspection + selective deletion.
//!
//! Demonstrates:
//! - `peek_dlq` with a 30-second visibility window so receipt handles stay
//!   exclusively valid long enough to decide what to do.
//! - `delete_dlq_message` for entries an operator decides are unrecoverable
//!   (corrupt payload, test data, irrelevant entries).
//!
//! Inspection logic in this example: any message whose body contains the
//! substring "fail-this" is treated as junk and deleted. Real operators
//! would inspect bodies manually or via more complex rules.
//!
//! Run with:
//! ```
//! cargo run --example dlq_peek_and_delete
//! ```
//!
//! Use it after `dlq_wired` has populated the DLQ.

use anyhow::Result;
use aws_pubsub_lite::{
    PubSub,
    models::{QueueDlq, ResourceName, ResourceNamingOptions, ResourceType, SeparatorSymbol},
    settings::{AwsSettings, QueueSettings},
    utils::new_sdk_config,
};

const PEEK_BATCH: i32 = 10;
const PEEK_VISIBILITY_SECS: i32 = 30;
const MAX_RECEIVE_COUNT: u32 = 3;

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
    let dlq = pubsub
        .add_dlq(ResourceName::new("dlq", ResourceType::Queue, naming())?)
        .await?;
    let _queue = pubsub
        .add_queue(
            ResourceName::new("queue", ResourceType::Queue, naming())?,
            topic.arn(),
            Some(QueueDlq::new(dlq.arn(), MAX_RECEIVE_COUNT)),
        )
        .await?;

    println!("DLQ URL: {}", dlq.url());
    println!();
    println!("Peeking up to {PEEK_BATCH} messages (visibility window: {PEEK_VISIBILITY_SECS}s)...");

    let messages = pubsub
        .peek_dlq(dlq.url(), PEEK_BATCH, PEEK_VISIBILITY_SECS)
        .await?;

    if messages.is_empty() {
        println!("DLQ is empty.");
        return Ok(());
    }

    println!("Found {} message(s):", messages.len());
    println!();

    let mut deleted = 0u32;
    for (i, msg) in messages.iter().enumerate() {
        // Body is the raw SNS->SQS envelope; show the inner Message field if
        // we can parse it, otherwise print the body's length.
        let preview = match serde_json::from_str::<serde_json::Value>(msg.body()) {
            Ok(env) => env["Message"]
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<no Message field>".to_string()),
            Err(_) => format!("<non-JSON body, len={}>", msg.body().len()),
        };
        println!("  [{i}] {preview}");

        // Operator decision: delete if it looks like junk.
        if preview.contains("fail-this") {
            println!("       -> deleting (matched 'fail-this')");
            pubsub
                .delete_dlq_message(dlq.url(), msg.receipt_handle())
                .await?;
            deleted += 1;
        }
    }

    println!();
    println!("Deleted {deleted} message(s) from the DLQ.");
    if deleted < messages.len() as u32 {
        let remaining = messages.len() as u32 - deleted;
        println!("{remaining} message(s) left visible after the {PEEK_VISIBILITY_SECS}s window expires.");
    }

    Ok(())
}
