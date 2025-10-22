# aws-pubsub-lite

A lightweight Rust library that wraps AWS SNS (topics) and SQS (queues) into a
single pub/sub abstraction. Manages topic creation, queue creation, message
publishing, polling consumption, and handler dispatch — all through one
`PubSub` type.

## Features

- **Single orchestrator** — `PubSub` owns topics and queues.
- **Idempotent provisioning** — every cold start reconciles to the desired AWS
  state.
- **Concurrent-safe** — `RwLock<HashMap<String, Arc<T>>>` registries, lock-free
  `AtomicBool` for subscription state, no async-mutex footguns.
- **Long polling by default** — SQS `ReceiveMessage` with 20-second wait, no
  client-side throttling.
- **Handler isolation** — panics in user handlers are caught and logged, the
  worker keeps running.
- **Parallel or sequential handlers** — pick per `subscribe` call via
  `HandlerExecutionMode`.
- **Graceful shutdown** — `CancellationToken`-driven, mid-handler abort via
  `JoinSet::shutdown`.
- **Typed errors throughout** — `thiserror`-based, with SDK error chains
  preserved via `#[source]`.
- **Body scrubbing** — message bodies never appear in `Display` or default
  tracing output, only `len=N`.

## Quick start

### Add to `Cargo.toml`

```toml
[dependencies]
aws-pubsub-lite = { path = "../aws-pubsub-lite" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tokio-util = "0.7"
anyhow = "1"
```

### Minimal usage

```rust
use std::sync::Arc;
use aws_pubsub_lite::{
    PubSub,
    errors::HandlerError,
    models::{
        BaseMessageHandler, HandlerExecutionMode, IncomingMessage, MessageDeleteMode,
        ResourceName, ResourceNamingOptions, ResourceType, SeparatorSymbol,
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

    let queue = pubsub.add_queue(
        ResourceName::new("orders", ResourceType::Queue, naming())?,
        topic.arn(),
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

The library reads configuration from environment variables (typically via
`dotenv` and a `.env` file):

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=eu-north-1

# Required for any subscribe / add_queue call
QUEUE_MAX_MESSAGE_COUNT=1                    # ReceiveMessage batch size (1-10)
QUEUE_RETRY_COUNT=15                         # Poll retry attempts
QUEUE_RETRY_INTERVAL_MS=2000                 # Poll-retry exponential backoff base
QUEUE_MESSAGE_RETENTION_PERIOD_MS=600000     # Main queue retention (10 min)
QUEUE_WAIT_TIME_SECONDS=20                   # SQS long-poll (0-20)
QUEUE_VISIBILITY_TIMEOUT_SECS=60             # Main queue visibility timeout

# Optional — only needed if PubSub::new is called with Some(TopicSettings)
TOPIC_MIN_DELAY_TARGET_SECS=5
TOPIC_MAX_DELAY_TARGET_SECS=60
TOPIC_NUM_RETRIES=3
TOPIC_NUM_MAX_DELAY_RETRIES=0
TOPIC_NUM_NO_DELAY_RETRIES=0
TOPIC_NUM_MIN_DELAY_RETRIES=0
TOPIC_BACKOFF_FUNCTION=linear                # linear|arithmetic|geometric|exponential
```

> **Important:** Make sure `.env` is in `.gitignore`. AWS credentials should
> never land in version control.

## How it works

### Provisioning (cold-start reconciliation)

Every call to `add_topic` / `add_queue` is idempotent on AWS *and* in-process:

1. **Read-lock pre-lookup** — if the resource is already in the in-process
   registry, return the cached `Arc` with no AWS calls.
2. **AWS work outside any lock** — `CreateTopic` / `CreateQueue` (idempotent on
   AWS), `SetTopicAttributes` / `SetQueueAttributes`.
3. **Race-safe insert** under a brief write lock via `entry().or_insert()`.

Manual operator changes to queue attributes are reverted on the next start
(the application is the source of truth — IaC-style).

### Subscribe lifecycle

`subscribe(queue, handlers, exec_mode, delete_mode, cancellation_token)`:

1. Looks up the `Arc<Queue>` under a read lock.
2. Atomic CAS on `Queue.subscribed` — fails fast with `QueueAlreadySubscribed`
   if already subscribed in this process.
3. **`ListSubscriptionsByTopic`** — only calls `Subscribe` if no
   `(sqs, queue_arn)` match exists, making restarts cheap.
4. Drives the polling stream in a `tokio::select!` loop with cancellation.
5. Per message:
   - Spawns handlers in a `JoinSet` (1 at a time for `Sequential`, all up
     front for `Parallel`).
   - Drains the `JoinSet` with cancellation racing `join_next`.
   - On cancellation, `JoinSet::shutdown()` aborts in-flight handlers cleanly.
   - Decides delete based on `delete_mode` and `all_handled`.

## Idempotency contract

Every `BaseMessageHandler` impl **must**:

1. Be idempotent — applying the same message twice produces the same durable
   result.
2. Return `HandlerError::ProcessedAlready` when a duplicate is detected.

The library uses rule 2 to safely delete duplicates under `DeleteAllHandled`.
SQS guarantees at-least-once delivery, so duplicates are normal, not
exceptional.

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
        // ... do real work ...
        self.dedup_store.insert(id);
        Ok(())
    })
}
```

## Build

```bash
cargo build           # library
cargo check
cargo clippy
cargo fmt
```

Rust **edition 2024** is required (Rust 1.85+).

## Status & caveats

- **Subscribe is one-shot per process.** No dynamic add/remove of handlers.
  Build that on top if you need it.
- **No tests yet.** The pure logic is testable; AWS-touching code requires
  LocalStack or similar.
- **Cold-start re-applies queue attributes.** Manual operator tweaks get
  reverted (this is intentional — see "Provisioning" above). If you need
  per-environment overrides, use environment variables, not console edits.
- **Handler panics are caught**, but a panicking handler still flips
  `all_handled = false` (under `DeleteAllHandled`, the message redelivers).
  Fix your handlers; don't rely on panic-as-control-flow.

## Roadmap

- **Dead-letter queue (DLQ) support** — provisioning via `add_dlq`, redrive
  attachment on `add_queue`, operator-driven `peek_dlq` / `drain_dlq` /
  `delete_dlq_message`. Coming in the next release.
- **Per-handler timeout** — bound each handler invocation so a hung handler
  cannot block the worker until visibility-timeout expiry.
- **Batch publish / receive** — `PublishBatch`, `SendMessageBatch`,
  `DeleteMessageBatch` for sustained throughput.
- **SNS subscription filter policies** — server-side fan-out filtering.
- **FIFO support** — strict per-key ordering.

## License

MIT — see [`LICENSE`](./LICENSE).
