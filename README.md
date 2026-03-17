# aws-pubsub-lite

A lightweight Rust library that wraps AWS SNS (topics) and SQS (queues) into a single pub/sub abstraction. Manages topic creation, queue creation, DLQ provisioning + redrive wiring, message publishing, polling consumption, and handler dispatch â€” all through one `PubSub` type.

## Features

- **Single orchestrator** â€” `PubSub` owns topics, queues, and DLQs.
- **Idempotent provisioning** â€” every cold start reconciles to the desired AWS state.
- **DLQ as a built-in feature** â€” provision via `add_dlq`, attach via `Option<QueueDlq>` on `add_queue`, recover via operator-triggered `peek_dlq` / `drain_dlq` / `delete_dlq_message`.
- **Concurrent-safe** â€” `RwLock<HashMap<String, Arc<T>>>` registries, lock-free `AtomicBool` for subscription state, no async-mutex footguns.
- **Long polling by default** â€” SQS `ReceiveMessage` with 20-second wait, no client-side throttling.
- **Handler isolation** â€” panics in user handlers are caught and logged, the worker keeps running.
- **Parallel or sequential handlers** â€” pick per `subscribe` call via `HandlerExecutionMode`.
- **Graceful shutdown** â€” `CancellationToken`-driven, mid-handler abort via `JoinSet::shutdown`.
- **Typed errors throughout** â€” `thiserror`-based, with SDK error chains preserved via `#[source]`.
- **Body scrubbing** â€” message bodies never appear in `Display` or default tracing output, only `len=N`.

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

The library reads configuration from environment variables (typically via `dotenv` and a `.env` file):

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=eu-north-1

# Required for any subscribe / add_queue / add_dlq call
QUEUE_MAX_MESSAGE_COUNT=1                    # ReceiveMessage batch size (1-10)
QUEUE_RETRY_COUNT=15                         # Poll retry attempts
QUEUE_RETRY_INTERVAL_MS=2000                 # Poll-retry exponential backoff base
QUEUE_MESSAGE_RETENTION_PERIOD_MS=600000     # Main queue retention (10 min)
QUEUE_DLQ_MESSAGE_RETENTION_PERIOD_MS=1209600000  # DLQ retention (14 days)
QUEUE_WAIT_TIME_SECONDS=20                   # SQS long-poll (0-20)
QUEUE_VISIBILITY_TIMEOUT_SECS=60             # Main queue visibility timeout

# Optional â€” only needed if PubSub::new is called with Some(TopicSettings)
TOPIC_MIN_DELAY_TARGET_SECS=5
TOPIC_MAX_DELAY_TARGET_SECS=60
TOPIC_NUM_RETRIES=3
TOPIC_NUM_MAX_DELAY_RETRIES=0
TOPIC_NUM_NO_DELAY_RETRIES=0
TOPIC_NUM_MIN_DELAY_RETRIES=0
TOPIC_BACKOFF_FUNCTION=linear                # linear|arithmetic|geometric|exponential
```

> **Important:** Make sure `.env` is in `.gitignore`. AWS credentials should never land in version control.

### DLQ tuning math

The math you must satisfy for failed messages to actually reach the DLQ:

```
QUEUE_MESSAGE_RETENTION_PERIOD_MS / 1000  /  QUEUE_VISIBILITY_TIMEOUT_SECS  >=  maxReceiveCount
```

With the defaults above (`600s / 60s = 10`), you can safely set `maxReceiveCount` up to 10. Higher values cause messages to be purged from the main queue before AWS gets a chance to route them to the DLQ.

## How it works

### Provisioning (cold-start reconciliation)

Every call to `add_topic` / `add_queue` / `add_dlq` is idempotent on AWS *and* in-process:

1. **Read-lock pre-lookup** â€” if the resource is already in the in-process registry, return the cached `Arc` with no AWS calls.
2. **AWS work outside any lock** â€” `CreateTopic` / `CreateQueue` (idempotent on AWS), `SetTopicAttributes` / `SetQueueAttributes`.
3. **Race-safe insert** under a brief write lock via `entry().or_insert()`.

Manual operator changes to queue attributes are reverted on the next start (the application is the source of truth â€” IaC-style).

### Subscribe lifecycle

`subscribe(queue, handlers, exec_mode, delete_mode, cancellation_token)`:

1. Looks up the `Arc<Queue>` under a read lock.
2. Atomic CAS on `Queue.subscribed` â€” fails fast with `QueueAlreadySubscribed` if already subscribed in this process.
3. **`ListSubscriptionsByTopic`** â€” only calls `Subscribe` if no `(sqs, queue_arn)` match exists, making restarts cheap.
4. Drives the polling stream in a `tokio::select!` loop with cancellation.
5. Per message:
   - Spawns handlers in a `JoinSet` (1 at a time for `Sequential`, all up front for `Parallel`).
   - Drains the `JoinSet` with cancellation racing `join_next`.
   - On cancellation, `JoinSet::shutdown()` aborts in-flight handlers cleanly.
   - Decides delete based on `delete_mode` and `all_handled`.

### DLQ flow at runtime

**Routing is purely AWS-driven.** The library never moves messages to a DLQ at runtime:

1. Worker receives a message, handler returns `HandlerError::ProcessingMessage`.
2. Under `DeleteAllHandled`, the library does not delete.
3. Visibility timeout expires â†’ message reappears â†’ received again â†’ SQS internal `ApproximateReceiveCount` increments.
4. After `maxReceiveCount` deliveries, **AWS** moves the message to the DLQ.

The library's responsibility is "don't delete on failure." Everything else is AWS plumbing fed by the `RedrivePolicy` attribute we wrote when `add_queue` was called with `Some(QueueDlq)`.

### DLQ recovery (operator-triggered)

| Method | Purpose |
|---|---|
| `peek_dlq(dlq_url, max, visibility_timeout_secs)` | Inspect messages without consuming them. Use `0` for true read-only or a small window (e.g., 30) to claim messages for inspect-then-delete workflows. |
| `drain_dlq(dlq_url, target_url, max_messages)` | Replay messages from a DLQ back to a target queue. Safe-drain semantics: send-then-delete per message; partial failures leave the DLQ copy alone for retry. |
| `delete_dlq_message(dlq_url, receipt_handle)` | Drop a specific message from a DLQ. Pair with `peek_dlq`. |

None of these run automatically. The library deliberately doesn't auto-drain on startup or auto-route at runtime â€” those decisions belong to operators.

## Idempotency contract

Every `BaseMessageHandler` impl **must**:

1. Be idempotent â€” applying the same message twice produces the same durable result.
2. Return `HandlerError::ProcessedAlready` when a duplicate is detected.

The library uses rule 2 to safely delete duplicates under `DeleteAllHandled`. Without it, duplicates from `drain_dlq` replays or concurrent retries can't be cleanly removed.

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

See `examples/idempotent_handler.rs` for a working example.

## Examples

The `examples/` directory contains end-to-end demonstrations of common patterns:

| Example | What it shows |
|---|---|
| `dlq_wired` | Full DLQ provisioning + redrive wiring. Handler that fails on certain payloads. |
| `idempotent_handler` | Concrete `ProcessedAlready` flow with an in-memory dedup store. |
| `parallel_handlers` | `Sequential` vs `Parallel` execution mode timing comparison. |
| `dlq_drain_operator` | Operator tool: drain a DLQ back to its main queue. |
| `dlq_peek_and_delete` | Operator tool: inspect DLQ + selectively delete junk messages. |
| `graceful_shutdown` | `Ctrl-C` â†’ cancellation token â†’ mid-handler abort. |

Run any of them:

```bash
cargo run --example dlq_wired
cargo run --example idempotent_handler
HANDLER_MODE=parallel cargo run --example parallel_handlers
cargo run --example dlq_peek_and_delete
cargo run --example dlq_drain_operator
cargo run --example graceful_shutdown   # then press Ctrl-C
```

The DLQ-related examples (`dlq_wired`, `dlq_peek_and_delete`, `dlq_drain_operator`) share resource names so they can be run in sequence to demonstrate the full failure-and-recovery flow.

## Built-in binaries

For ad-hoc testing:

```bash
cargo run --bin pub    # reads stdin, publishes each line to a hardcoded topic
cargo run --bin sub    # subscribes to a hardcoded queue and prints incoming messages
```

Use these to verify your environment is wired correctly before building your own integration.

## Build

```bash
cargo build           # library + binaries
cargo build --examples
cargo check
cargo clippy
cargo fmt
```

Rust **edition 2024** is required (Rust 1.85+).

## Architecture deep-dive


## Status & caveats

- **Subscribe is one-shot per process.** No dynamic add/remove of handlers. Build that on top if you need it.
- **No tests yet.** The pure logic is testable; AWS-touching code requires LocalStack or similar.
- **Cold-start re-applies queue attributes.** Manual operator tweaks get reverted (this is intentional â€” see "Provisioning" above). If you need per-environment overrides, use environment variables, not console edits.
- **Handler panics are caught**, but a panicking handler still flips `all_handled = false` (under `DeleteAllHandled`, the message redelivers). Fix your handlers; don't rely on panic-as-control-flow.

## Future features

The library covers the standard pub/sub flow well, but several features are still missing for high-scale or sophisticated production needs. Listed roughly in priority order:

### Highest leverage (would close most production gaps)

- **Message attributes API** â€” publish-side and receive-side support for SNS/SQS message attributes. Unlocks distributed-tracing context propagation (W3C / OpenTelemetry), content-type discrimination, and SNS subscription filter policies.
- **Per-handler timeout** â€” wrap each handler invocation in `tokio::time::timeout(...)`. Prevents a hung handler from blocking the worker until visibility-timeout expiry. Sensible default: 90% of `QUEUE_VISIBILITY_TIMEOUT_SECS`.
- **Metrics layer** â€” pluggable trait for emitting structured counters and histograms (messages-published, handler-duration, error-count by variant, DLQ-depth). Adapters for Prometheus and OpenTelemetry.
- **Batch operations** â€” `PublishBatch` (SNS, 10 msgs/call), `SendMessageBatch` (SQS), `DeleteMessageBatch`. For sustained > ~100 msg/s, batching is dramatically cheaper.
- **Retry filter on `is_retryable()`** â€” stop retrying permanent SDK errors (`AccessDenied`, `InvalidQueueUrl`). Currently every error gets the full retry budget.

### Messaging features

- **SNS subscription filter policies** â€” exposed via `add_queue` (or a separate method). Lets one topic fan out to multiple queues with each consumer receiving only the message types it cares about. Server-side filtering is much more efficient than receive-then-filter.
- **Multiple topics per queue** â€” a single queue subscribed to N topics. Useful for event-aggregation patterns. Today the model is 1 topic â†” 1 queue.
- **FIFO support** â€” strict per-key ordering via SNS FIFO topics + SQS FIFO queues. Required when ordering matters (per-customer event streams, per-aggregate event sourcing). FIFO requires `MessageGroupId`, optional `MessageDeduplicationId`, and a different name suffix (`.fifo`).
- **Large-message support** â€” S3-extended-client pattern for payloads exceeding the SQS 256 KB limit. Body stored in S3, message contains a pointer.

### Operations

- **"Soft cancel" mode** â€” instead of `JoinSet::shutdown` aborting in-flight handlers, stop receiving new messages, finish what's running, then exit. Pairs with SIGTERM in K8s.
- **N-way parallel consumers per queue per process** â€” currently `try_subscribe` enforces one subscriber per queue per process; horizontal scaling means more processes. A `subscribe_with_concurrency(n)` mode would let one process run N receive-and-dispatch loops against the same queue.
- **Dynamic handler registration** â€” add/remove `BaseMessageHandler` impls at runtime without restarting `subscribe`. Useful for plugin-style architectures.

### Compliance / regulated workloads

- **SSE-KMS configuration** â€” `QueueSettings::kms_master_key_id` to enable SQS server-side encryption with a customer-managed KMS key. Required for many compliance regimes (PII, PCI, HIPAA).
- **Topic-level encryption** â€” equivalent for SNS topics via `KmsMasterKeyId` topic attribute.

### Lower priority / niche

- **`destroy_dlq`** â€” programmatic DLQ teardown. Skipped because IaC usually owns DLQ lifecycle and destroying a DLQ destroys forensic evidence.
- **Auto-reconciliation of redrive policy** â€” currently re-applied on every `add_queue`; could be opt-out for IaC-only environments.
- **Tests** â€” pure logic is trivially testable (resource-name validation, redrive-policy JSON, mode precedence). Integration tests against LocalStack.

If any of these are blocking your use case, they're additions rather than rewrites â€” the existing architecture accommodates each cleanly.

## License

(Add your license of choice here â€” MIT / Apache-2.0 / etc.)
