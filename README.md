# aws-pubsub-lite

A small Rust library that puts AWS SNS and SQS behind one simple pub/sub API.

Topics are SNS. Queues are SQS. Dead-letter queues are SQS queues wired with a
redrive policy. One type, `PubSub`, creates these resources and handles
publishing and consuming for you.

## What you get

- Creates topics, queues and DLQs for you. It is safe to run on every start.
- Many queues can listen to one topic.
- DLQ support, with helpers to peek, drain and delete messages.
- Long polling with retries.
- Your own message handlers through the `BaseMessageHandler` trait.
- Clean shutdown with a `CancellationToken`.
- Typed errors and logging with `tracing`.

## Install

```toml
[dependencies]
aws-pubsub-lite = { git = "https://github.com/bnkselfin/aws-pubsub-lite" }
```

## Configuration

Settings are read from environment variables. The easiest way is a `.env` file
loaded with `dotenv`. Every variable starts with `PUBSUB_`.

```
PUBSUB_AWS_ACCESS_KEY_ID=...
PUBSUB_AWS_SECRET_ACCESS_KEY=...
PUBSUB_AWS_REGION=eu-north-1

PUBSUB_QUEUE_MAX_MESSAGE_COUNT=1
PUBSUB_QUEUE_RETRY_COUNT=15
PUBSUB_QUEUE_RETRY_INTERVAL_MS=2000
PUBSUB_QUEUE_MESSAGE_RETENTION_PERIOD_MS=600000
PUBSUB_QUEUE_DLQ_MESSAGE_RETENTION_PERIOD_MS=1209600000
PUBSUB_QUEUE_WAIT_TIME_SECONDS=20
PUBSUB_QUEUE_VISIBILITY_TIMEOUT_SECS=60
```

Topic delivery settings are optional. If you need them:

```
PUBSUB_TOPIC_MIN_DELAY_TARGET_SECS=5
PUBSUB_TOPIC_MAX_DELAY_TARGET_SECS=60
PUBSUB_TOPIC_NUM_RETRIES=3
PUBSUB_TOPIC_NUM_MAX_DELAY_RETRIES=0
PUBSUB_TOPIC_NUM_NO_DELAY_RETRIES=0
PUBSUB_TOPIC_NUM_MIN_DELAY_RETRIES=0
PUBSUB_TOPIC_BACKOFF_FUNCTION=linear
```

Keep `.env` out of git. Never commit real AWS keys.

## Publish

See the `examples/` folder for full programs. A small publisher looks like this:

```rust
use aws_pubsub_lite::{
    models::{ResourceName, ResourceType, ResourceNamingOptions},
    settings::AwsSettings,
    utils::new_sdk_config,
    PubSub,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let aws = AwsSettings::from_env()?;
    let cfg = new_sdk_config(&aws).await?;
    let ps = PubSub::new(None, None, &cfg)?;

    let topic = ps
        .add_topic(ResourceName::new(
            "orders",
            ResourceType::Topic,
            ResourceNamingOptions::new(None, None, None, None),
        )?)
        .await?;

    ps.publish(topic.name(), "hello").await?;
    Ok(())
}
```

## A note on DLQ size

For the DLQ to work, this must be true:

```
(PUBSUB_QUEUE_MESSAGE_RETENTION_PERIOD_MS / 1000) / PUBSUB_QUEUE_VISIBILITY_TIMEOUT_SECS >= maxReceiveCount
```

If not, messages can be dropped before AWS moves them to the DLQ. With the
defaults that is 600 / 60 = 10, so a maxReceiveCount up to 10 is fine.

## License

MIT. See the LICENSE file.