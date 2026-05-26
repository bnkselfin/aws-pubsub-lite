# aws-pubsub-lite

A lightweight Rust library that wraps AWS SNS (topics) and SQS (queues) into a single `PubSub` abstraction. It owns topic/queue/DLQ provisioning, redrive wiring, publishing, polling, and handler dispatch.

## Features

- **Single orchestrator** — `PubSub` owns topics, queues, and DLQs.
- **Idempotent provisioning** — every cold start reconciles to the desired AWS state.
- **Built-in DLQ** — provision via `add_dlq`, attach via `Option<QueueDlq>`, recover via `peek_dlq` / `drain_dlq` / `delete_dlq_message`.
- **Concurrent-safe** — `RwLock` registries, lock-free `AtomicBool` subscription state, no async-mutex footguns.
- **Long polling by default** — 20s SQS wait, no client-side throttling.
- **Handler isolation** — panics are caught and logged; the worker keeps running.
- **Parallel or sequential handlers** — chosen per `subscribe` call.
- **Graceful shutdown** — `CancellationToken`-driven, mid-handler abort via `JoinSet::shutdown`.
- **Typed errors** — `thiserror`-based with SDK error chains preserved; message bodies scrubbed to `len=N`.

## Quick start

`Cargo.toml`:

```toml
[dependencies]
aws-pubsub-lite = { path = "../aws-pubsub-lite" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tokio-util = "0.7"
anyhow = "1"
```

Minimal usage:

```rust
use std::sync::Arc;
use aws_pubsub_lite::{
    PubSub,
    errors::HandlerError,
    models::{
        BaseMessageHandler, HandlerExecutionMode, IncomingMessage, MessageDeleteMode,
        QueueDlq, ResourceName, ResourceNamingOptions, ResourceType, SeparatorSymbol,
    },
    settings::{AwsSettings, QueueSettings},
    utils::new_sdk_config,
};
use tokio_util::sync::CancellationToken;

struct MyHandler;

impl BaseMessageHandler for MyHandler {
    fn handler_name(&self) -> &'static str { "MyHandler" }
    fn handle(
        &self,
        message: &IncomingMessage,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + '_>> {
        let body = message.body().to_string();
        Box::pin(async move {
            println!("got: {body}");
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let aws_settings = AwsSettings::from_env()?;
    let queue_settings = QueueSettings::from_env()?;
    let aws_config = new_sdk_config(&aws_settings).await?;
    let pubsub = PubSub::new(Some(queue_settings), None, aws_config).await?;

    let naming = || ResourceNamingOptions::new(
        Some("v1"), Some("topic"),
        Some(SeparatorSymbol::Hyphen), Some(SeparatorSymbol::Underscore),
    );

    let topic = pubsub.add_topic(
        ResourceName::new("orders", ResourceType::Topic, naming())?
    ).await?;
    let dlq = pubsub.add_dlq(
        ResourceName::new("orders-dlq", ResourceType::Queue, naming())?
    ).await?;
    let queue = pubsub.add_queue(
        ResourceName::new("orders", ResourceType::Queue, naming())?,
        topic.arn(),
        Some(QueueDlq::new(dlq.arn(), 5)),
    ).await?;

    let handlers: Vec<Arc<dyn BaseMessageHandler>> = vec![Arc::new(MyHandler)];
    pubsub.subscribe(
        queue.name(),
        handlers,
        HandlerExecutionMode::Sequential,
        MessageDeleteMode::DeleteAllHandled,
        CancellationToken::new(),
    ).await?;

    Ok(())
}
```

## Configuration

Configuration is read from environment variables (typically via `dotenv` + a `.env` file):

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=eu-north-1

QUEUE_MAX_MESSAGE_COUNT=1
QUEUE_RETRY_COUNT=15
QUEUE_RETRY_INTERVAL_MS=2000
QUEUE_MESSAGE_RETENTION_PERIOD_MS=600000
QUEUE_DLQ_MESSAGE_RETENTION_PERIOD_MS=1209600000
QUEUE_WAIT_TIME_SECONDS=20
QUEUE_VISIBILITY_TIMEOUT_SECS=60

TOPIC_MIN_DELAY_TARGET_SECS=5
TOPIC_MAX_DELAY_TARGET_SECS=60
TOPIC_NUM_RETRIES=3
TOPIC_NUM_MAX_DELAY_RETRIES=0
TOPIC_NUM_NO_DELAY_RETRIES=0
TOPIC_NUM_MIN_DELAY_RETRIES=0
TOPIC_BACKOFF_FUNCTION=linear
```

> Keep `.env` in `.gitignore` — AWS credentials must never land in version control.

**DLQ tuning:** `(QUEUE_MESSAGE_RETENTION_PERIOD_MS / 1000) / QUEUE_VISIBILITY_TIMEOUT_SECS >= maxReceiveCount` must hold, or messages get purged before AWS routes them to the DLQ. Defaults give `600 / 60 = 10`, so `maxReceiveCount` up to 10 is safe.

## How it works

- **Provisioning** — `add_topic` / `add_queue` / `add_dlq` are idempotent on AWS and in-process: read-lock pre-lookup, AWS work outside any lock, race-safe insert. Manual queue-attribute edits are reverted on the next start (the app is the source of truth, IaC-style).
- **Subscribe** — looks up the queue, atomic CAS on `subscribed` (fails fast with `QueueAlreadySubscribed`), subscribes to SNS only if no matching subscription exists, then drives the polling stream in a `tokio::select!` loop. Per message, handlers run in a `JoinSet` and delete is decided by `delete_mode` + `all_handled`.
- **DLQ routing** — purely AWS-driven. On failure the library just doesn't delete; after `maxReceiveCount` deliveries AWS moves the message to the DLQ via the `RedrivePolicy` written at `add_queue`.
- **DLQ recovery** — operator-triggered only: `peek_dlq` (inspect), `drain_dlq` (replay to a target queue, send-then-delete), `delete_dlq_message` (drop one). Nothing runs automatically.

## Idempotency contract

Every `BaseMessageHandler` **must** be idempotent and return `HandlerError::ProcessedAlready` when it detects a duplicate. The library uses that signal to safely delete duplicates under `DeleteAllHandled` (from `drain_dlq` replays or concurrent retries).

```rust
fn handle(&self, message: &IncomingMessage) -> /* ... */ {
    Box::pin(async move {
        let id = parse_id_from(message.body())?;
        if self.dedup_store.contains(&id) {
            return Err(HandlerError::ProcessedAlready {
                message: format!("id={id}"),
                handler: "MyHandler".to_string(),
            });
        }
        self.dedup_store.insert(id);
        Ok(())
    })
}
```

## Examples

```bash
cargo run --example dlq_wired
cargo run --example idempotent_handler
HANDLER_MODE=parallel cargo run --example parallel_handlers
cargo run --example dlq_peek_and_delete
cargo run --example dlq_drain_operator
cargo run --example graceful_shutdown
```

The DLQ examples share resource names, so run them in sequence to demonstrate the full failure-and-recovery flow.

Ad-hoc binaries: `cargo run --bin pub` (publish stdin lines) and `cargo run --bin sub` (print incoming messages).

## Build

```bash
cargo build
cargo build --examples
cargo check
cargo clippy
cargo fmt
```

Rust **edition 2024** is required (Rust 1.85+).

## Caveats

- Subscribe is one-shot per process — no dynamic add/remove of handlers.
- Cold-start re-applies queue attributes; manual console edits get reverted (use env vars for per-environment overrides).
- Handler panics are caught but still flip `all_handled = false` (the message redelivers under `DeleteAllHandled`).
- No tests yet — pure logic is testable; AWS-touching code needs LocalStack or similar.

## License

Licensed under the [MIT License](./LICENSE). Copyright (c) 2026 bnkselfin.
