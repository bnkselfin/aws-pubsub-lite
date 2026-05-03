//! Operator-style DLQ drain tool.
//!
//! Drains messages from the `examples-dlq-wired` DLQ back to the main queue
//! created by the `dlq_wired` example. Idempotent — running it again after
//! all DLQ messages are gone simply returns 0.
//!
//! Workflow this models:
//! 1. Bug deployed -> messages fail handler -> DLQ fills up.
//! 2. Operator fixes the bug, redeploys.
//! 3. Operator runs this tool to replay failed messages back through the
//!    fixed handler.
//!
//! Run with:
//! ```
//! cargo run --example dlq_drain_operator
//! ```
//!
//! Important: `drain_dlq` only re-sends messages. It does NOT process them.
//! Run `dlq_wired` (or whatever your subscriber is) to consume the drained
//! messages from the main queue.

use anyhow::Result;
use aws_pubsub_lite::{
    PubSub,
    models::{QueueDlq, ResourceName, ResourceNamingOptions, ResourceType, SeparatorSymbol},
    settings::{AwsSettings, QueueSettings},
    utils::new_sdk_config,
};

const MAX_RECEIVE_COUNT: u32 = 3;
const MAX_DRAIN_PER_RUN: u32 = 1000;

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

    // Re-resolve the same topic / queue / DLQ that dlq_wired created.
    // The add_* methods are idempotent — they short-circuit on the in-memory
    // pre-lookup OR on AWS-side idempotency for fresh processes.
    let topic = pubsub
        .add_topic(ResourceName::new("topic", ResourceType::Topic, naming())?)
        .await?;
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

    println!("DLQ URL:        {}", dlq.url());
    println!("Target queue:   {}", queue.url());
    println!("Max per run:    {MAX_DRAIN_PER_RUN}");
    println!();

    let drained = pubsub
        .drain_dlq(dlq.url(), queue.url(), MAX_DRAIN_PER_RUN)
        .await?;
    println!("Drained {drained} message(s) from DLQ -> main queue.");

    if drained > 0 {
        println!();
        println!("Next: run a subscriber against the main queue to consume the drained messages.");
        println!("If your handler is fixed, they should process successfully this time.");
    }

    Ok(())
}
