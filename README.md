# aws-pubsub-lite

A lightweight Rust library that wraps AWS SNS (topics) and SQS (queues) into a
single pub/sub abstraction.

## Status

Early scaffolding. Public API surface, examples, and architecture notes will
land as the crate matures.

## Planned scope

- Single orchestrator type that owns topics, queues, and their wiring.
- Idempotent provisioning on cold start (CreateTopic, CreateQueue,
  SetQueueAttributes, ListSubscriptionsByTopic before Subscribe).
- Long-polling consumer with configurable retry strategy.
- Object-safe handler trait with sequential or parallel dispatch per message.
- Typed error model with `thiserror`-derived enums and SDK error chains
  preserved via `#[source]`.
- Body scrubbing — message payloads never appear in `Display` output.

## Quick start

_To be written once the public API stabilizes._

## License

MIT — see [`LICENSE`](./LICENSE).
