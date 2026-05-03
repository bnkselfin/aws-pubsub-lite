//! Graceful shutdown via Ctrl-C and `CancellationToken`.
//!
//! Demonstrates:
//! - Wiring `tokio::signal::ctrl_c()` to the `CancellationToken` passed to
//!   `subscribe`.
//! - Mid-handler abort via `JoinSet::shutdown` when the token fires.
//! - Clean unsubscribe on exit (no leaked SNS subscriptions, no orphan
//!   in-process state).
//!
//! The example handler sleeps for 5 seconds per message, simulating a slow
//! task. Ctrl-C while a handler is running causes the in-flight task to be
//! aborted (you'll see "Handler aborted" in the WARN log), the message
//! deletion is skipped, and the message will redeliver after the visibility
//! timeout.
//!
//! Run with:
//! ```
//! cargo run --example graceful_shutdown
//! # then publish from another terminal, or wait for the auto-publish below
//! # then press Ctrl-C
//! ```

use std::sync::Arc;
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

struct SlowHandler;

impl BaseMessageHandler for SlowHandler {
    fn handler_name(&self) -> &'static str {
        "SlowHandler"
    }

    fn handle(
        &self,
        _message: &IncomingMessage,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + '_>> {
        Box::pin(async move {
            println!("[SlowHandler] starting (will take 5s)");
            tokio::time::sleep(Duration::from_secs(5)).await;
            println!("[SlowHandler] done");
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
            Some("graceful"),
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

    // Auto-publish a few messages so there's work to do.
    for i in 0..5 {
        pubsub.publish(topic.name(), &format!("msg-{i}")).await?;
    }
    println!("Published 5 messages. Press Ctrl-C to trigger graceful shutdown.");

    let cancel = CancellationToken::new();

    // Bridge Ctrl-C to the cancellation token.
    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("\nCtrl-C received. Cancelling subscription...");
            cancel_for_signal.cancel();
        }
    });

    let handlers: Vec<Arc<dyn BaseMessageHandler>> = vec![Arc::new(SlowHandler)];
    pubsub
        .subscribe(
            queue.name(),
            handlers,
            HandlerExecutionMode::Sequential,
            MessageDeleteMode::DeleteAllHandled,
            cancel,
        )
        .await?;

    println!("Subscribe loop ended cleanly. Exiting.");
    Ok(())
}
