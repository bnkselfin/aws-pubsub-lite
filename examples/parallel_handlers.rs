//! Parallel vs Sequential handler execution.
//!
//! Demonstrates `HandlerExecutionMode::Parallel` for independent handlers.
//! Three handlers each sleep ~200ms (simulated work). Per message:
//! - Sequential: ~600ms wall time (1 + 1 + 1 = 3 chained awaits)
//! - Parallel:   ~200ms wall time (max of three concurrent sleeps)
//!
//! Run with:
//! ```
//! HANDLER_MODE=sequential cargo run --example parallel_handlers
//! HANDLER_MODE=parallel   cargo run --example parallel_handlers
//! ```
//! and compare per-message timings printed by each handler.

use std::sync::Arc;
use std::time::{Duration, Instant};

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

struct SlowHandler {
    name: &'static str,
    delay_ms: u64,
}

impl BaseMessageHandler for SlowHandler {
    fn handler_name(&self) -> &'static str {
        self.name
    }

    fn handle(
        &self,
        _message: &IncomingMessage,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + '_>> {
        let name = self.name;
        let delay = Duration::from_millis(self.delay_ms);
        Box::pin(async move {
            let started = Instant::now();
            tokio::time::sleep(delay).await;
            println!("[{name}] done in {:?}", started.elapsed());
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let mode = match std::env::var("HANDLER_MODE").as_deref() {
        Ok("parallel") => HandlerExecutionMode::Parallel,
        _ => HandlerExecutionMode::Sequential,
    };
    println!("Running with mode: {mode:?}");

    let aws_settings = AwsSettings::from_env()?;
    let queue_settings = QueueSettings::from_env()?;
    let aws_config = new_sdk_config(&aws_settings).await?;

    let pubsub = PubSub::new(Some(queue_settings), None, aws_config).await?;

    let naming = || {
        ResourceNamingOptions::new(
            Some("examples"),
            Some("parallel"),
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

    pubsub.publish(topic.name(), "trigger").await?;
    println!("Published 1 message");

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(20)).await;
        cancel_clone.cancel();
    });

    let handlers: Vec<Arc<dyn BaseMessageHandler>> = vec![
        Arc::new(SlowHandler {
            name: "Slow-A",
            delay_ms: 200,
        }),
        Arc::new(SlowHandler {
            name: "Slow-B",
            delay_ms: 200,
        }),
        Arc::new(SlowHandler {
            name: "Slow-C",
            delay_ms: 200,
        }),
    ];

    pubsub
        .subscribe(
            queue.name(),
            handlers,
            mode,
            MessageDeleteMode::DeleteAllCalled,
            cancel,
        )
        .await?;

    Ok(())
}
