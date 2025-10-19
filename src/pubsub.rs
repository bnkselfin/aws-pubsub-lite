use crate::errors::HandlerError;
use crate::errors::PubSubError;
use crate::models::BaseMessageHandler;
use crate::models::HandlerExecutionMode;
use crate::models::IncomingMessage;
use crate::models::MessageDeleteMode;
use crate::models::Queue;
use crate::models::ResourceName;
use crate::models::SnsTopicAttribute;
use crate::models::Topic;
use crate::settings::{QueueSettings, TopicSettings};
use aws_config::SdkConfig;
use aws_sdk_sns::{Client as SnsClient, operation::publish::PublishOutput};
use aws_sdk_sqs::{Client as SqsClient, types::QueueAttributeName};
use futures::StreamExt;
use futures::{Stream, stream};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use std::time::Duration;
use std::{collections::VecDeque};
use tokio::task::JoinSet;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct PubSub {
    sns_client: SnsClient,
    sqs_client: SqsClient,
    queue_settings: Mutex<Option<QueueSettings>>,
    topic_settings: Mutex<Option<TopicSettings>>,
    topics: RwLock<HashMap<String, Arc<Topic>>>,
    queues: RwLock<HashMap<String, Arc<Queue>>>,
}

impl PubSub {
    #[tracing::instrument(name = "PubSub::init", skip(aws_sdk_config))]
    pub async fn new(
        queue_settings: Option<QueueSettings>,
        topic_settings: Option<TopicSettings>,
        aws_sdk_config: SdkConfig,
    ) -> Result<Arc<PubSub>, PubSubError> {
        tracing::info!("Starting PubSub initialization");

        let sns_client = SnsClient::new(&aws_sdk_config);
        let sqs_client = SqsClient::new(&aws_sdk_config);

        tracing::info!("PubSub has been successfully initialized");

        Ok(Arc::new(Self {
            sns_client,
            sqs_client,
            queue_settings: Mutex::new(queue_settings),
            topic_settings: Mutex::new(topic_settings),
            topics: RwLock::new(HashMap::new()),
            queues: RwLock::new(HashMap::new()),
        }))
    }

    #[tracing::instrument(name = "PubSub::add_topic", skip(self))]
    pub async fn add_topic(&self, name: ResourceName) -> Result<Arc<Topic>, PubSubError> {
        let topic_name = name.to_string();

        if let Some(existing) = self
            .topics
            .read()
            .map_err(|_| PubSubError::LockPoisoned { resource: "topics" })?
            .get(&topic_name)
            .cloned()
        {
            return Ok(existing);
        }

        let sns_arn = self.set_sns(&topic_name).await?;
        let topic = Arc::new(Topic::new(name, &sns_arn)?);

        let mut topics = self
            .topics
            .write()
            .map_err(|_| PubSubError::LockPoisoned { resource: "topics" })?;
        Ok(topics.entry(topic_name).or_insert(topic).clone())
    }

    #[tracing::instrument(name = "PubSub::add_queue", skip(self))]
    pub async fn add_queue(
        &self,
        name: ResourceName,
        sns_arn: &str,
    ) -> Result<Arc<Queue>, PubSubError> {
        let queue_name = name.to_string();

        if let Some(existing) = self
            .queues
            .read()
            .map_err(|_| PubSubError::LockPoisoned { resource: "queues" })?
            .get(&queue_name)
            .cloned()
        {
            return Ok(existing);
        }

        let (sqs_arn, sqs_url) = self.set_sqs(&queue_name, sns_arn).await?;
        self.set_sqs_attributes(&sqs_arn, &sqs_url, sns_arn).await?;

        let queue = Arc::new(Queue::new(name, &sqs_arn, sqs_url, sns_arn)?);

        let mut queues = self
            .queues
            .write()
            .map_err(|_| PubSubError::LockPoisoned { resource: "queues" })?;
        Ok(queues.entry(queue_name).or_insert(queue).clone())
    }

    #[tracing::instrument(name = "PubSub::set_sns", skip(self))]
    async fn set_sns(&self, topic_name: &str) -> Result<String, PubSubError> {
        tracing::info!("Creating SNS topic");

        let response = self
            .sns_client
            .create_topic()
            .name(topic_name)
            .send()
            .await
            .map_err(|e| {
                tracing::error!(source = %e, "Failed to create SNS topic");
                PubSubError::SNSTopicCreation {
                    topic: topic_name.to_string(),
                    source: e,
                }
            })?;

        tracing::info!("SNS topic created successfully");

        let sns_arn = response
            .topic_arn()
            .ok_or_else(|| PubSubError::GettingSNSTopicArn(topic_name.to_string()))
            .inspect_err(|_| {
                tracing::error!("Failed to get SNS topic arn");
            })?
            .to_string();

        if let Some(topic_settings) = self.topic_settings()? {
            self.sns_client
                .set_topic_attributes()
                .topic_arn(&sns_arn)
                .attribute_name(SnsTopicAttribute::DeliveryPolicy.as_str())
                .attribute_value(topic_settings.delivery_policy_json())
                .send()
                .await
                .map_err(|e| PubSubError::SettingTopicAttributes {
                    topic_arn: sns_arn.clone(),
                    attribute: SnsTopicAttribute::DeliveryPolicy,
                    source: e,
                })?;
            tracing::info!("Applied DeliveryPolicy to SNS topic");
        }

        Ok(sns_arn)
    }

    #[tracing::instrument(name = "PubSub::set_sqs", skip(self))]
    async fn set_sqs(
        &self,
        queue_name: &str,
        sns_arn: &str,
    ) -> Result<(String, String), PubSubError> {
        tracing::info!("Creating SQS queue");

        let response = self
            .sqs_client
            .create_queue()
            .queue_name(queue_name)
            .send()
            .await
            .map_err(|e| {
                tracing::error!(source = %e, "Failed to create the queue");
                PubSubError::SQSQueueCreation {
                    sns_arn: sns_arn.to_string(),
                    queue: queue_name.to_string(),
                    source: e,
                }
            })?;

        let sqs_url = response
            .queue_url()
            .ok_or_else(|| PubSubError::GettingSQSQueueUrl(queue_name.to_string()))?
            .to_string();

        let sqs_arn = self
            .sqs_client
            .get_queue_attributes()
            .queue_url(&sqs_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| {
                tracing::error!(attribute = %QueueAttributeName::QueueArn, source = %e, "Failed to get the attribute of the queue");
                PubSubError::GettingQueueAttributes {
                    queue: queue_name.to_string(),
                    attribute: QueueAttributeName::QueueArn,
                    source: e,
                }
            })?
            .attributes()
            .ok_or_else(|| PubSubError::NoQueueAttributes(queue_name.to_string()))?
            .get(&QueueAttributeName::QueueArn)
            .ok_or_else(|| PubSubError::QueueArnNotFound(queue_name.to_string()))?
            .to_string();

        tracing::info!("SQS has been set successfully for the queue");

        Ok((sqs_arn, sqs_url))
    }

    #[tracing::instrument(name = "PubSub::set_sqs_attributes", skip(self))]
    async fn set_sqs_attributes(
        &self,
        sqs_arn: &str,
        sqs_url: &str,
        sns_arn: &str,
    ) -> Result<(), PubSubError> {
        let queue_settings = self.queue_settings("add_queue")?;
        let policy = format!(
            r#"{{
                "Version": "2012-10-17",
                "Statement": [
                    {{
                        "Sid": "AllowSNS",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "SQS:SendMessage",
                        "Resource": "{}",
                        "Condition": {{
                            "ArnEquals": {{
                                "aws:SourceArn": "{}"
                            }}
                        }}
                    }}
                ]
            }}"#,
            sqs_arn, sns_arn
        );

        self.sqs_client
            .set_queue_attributes()
            .queue_url(sqs_url)
            .attributes(
                QueueAttributeName::MessageRetentionPeriod,
                Duration::from_millis(queue_settings.message_retention_ms)
                    .as_secs()
                    .to_string(),
            )
            .attributes(
                QueueAttributeName::VisibilityTimeout,
                queue_settings.visibility_timeout_secs.to_string(),
            )
            .attributes(QueueAttributeName::Policy, policy)
            .send()
            .await
            .map_err(|e| PubSubError::SettingQueueAttributes {
                queue_arn: sqs_arn.to_string(),
                attribute: QueueAttributeName::Policy,
                source: e,
            })?;
        tracing::info!("Succeeded to set queue attributes");
        Ok(())
    }

    #[tracing::instrument(name = "PubSub::ensure_subscription", skip(self))]
    async fn ensure_subscription(
        &self,
        topic_arn: &str,
        queue_arn: &str,
    ) -> Result<(), PubSubError> {
        let mut next_token: Option<String> = None;
        loop {
            let mut request = self
                .sns_client
                .list_subscriptions_by_topic()
                .topic_arn(topic_arn);
            if let Some(token) = next_token.as_deref() {
                request = request.next_token(token);
            }

            let response = request.send().await.map_err(|e| {
                tracing::error!(source = %e, "Failed to list SNS subscriptions");
                PubSubError::ListingSubscriptions {
                    topic: topic_arn.to_string(),
                    source: e,
                }
            })?;

            for subscription in response.subscriptions() {
                if subscription.protocol() == Some("sqs")
                    && subscription.endpoint() == Some(queue_arn)
                {
                    tracing::info!(
                        topic = topic_arn,
                        queue = queue_arn,
                        "Subscription already exists, skipping Subscribe"
                    );
                    return Ok(());
                }
            }

            match response.next_token() {
                Some(token) => next_token = Some(token.to_string()),
                None => break,
            }
        }

        self.sns_client
            .subscribe()
            .topic_arn(topic_arn)
            .protocol("sqs")
            .endpoint(queue_arn)
            .send()
            .await
            .map_err(|e| PubSubError::SubscribingQueue {
                topic: topic_arn.to_string(),
                queue_arn: queue_arn.to_string(),
                source: e,
            })?;

        tracing::info!(
            topic = topic_arn,
            queue = queue_arn,
            "Created new SNS subscription"
        );
        Ok(())
    }

    #[tracing::instrument(name = "PubSub::publish", skip(self, msg), fields(msg_len = msg.len()))]
    pub async fn publish(&self, topic_name: &str, msg: &str) -> Result<PublishOutput, PubSubError> {
        if msg.is_empty() {
            return Err(PubSubError::EmptyMessage);
        }

        let topic_arn = {
            let topics = self
                .topics
                .read()
                .map_err(|_| PubSubError::LockPoisoned { resource: "topics" })?;
            topics
                .get(topic_name)
                .ok_or_else(|| PubSubError::TopicNotExists(topic_name.to_string()))?
                .arn()
                .to_string()
        };

        let publish_output: PublishOutput = self
            .sns_client
            .publish()
            .topic_arn(topic_arn)
            .message(msg)
            .send()
            .await
            .map_err(|e| PubSubError::PublishMessage {
                topic: topic_name.to_string(),
                message: msg.into(),
                source: e,
            })?;

        tracing::info!("Successfully pushed the message to the topic");
        Ok(publish_output)
    }

    #[tracing::instrument(name = "pubsub::subscribe", skip(self, queue_name, handlers, cancellation_token), fields(queue = %queue_name, subscribers = handlers
        .iter()
        .map(|h| h.handler_name())
        .collect::<Vec<_>>()
        .join(", "), subscriber_count = handlers.len()))]
    pub async fn subscribe(
        &self,
        queue_name: &str,
        handlers: Vec<Arc<dyn BaseMessageHandler>>,
        handler_execution_mode: HandlerExecutionMode,
        delete_mode: MessageDeleteMode,
        cancellation_token: CancellationToken,
    ) -> Result<(), PubSubError> {
        let queue_settings = self.queue_settings("subscribe")?;

        let queue = {
            let queues = self
                .queues
                .read()
                .map_err(|_| PubSubError::LockPoisoned { resource: "queues" })?;
            queues
                .get(queue_name)
                .cloned()
                .ok_or_else(|| PubSubError::QueueNotExists(queue_name.to_string()))?
        };

        if !queue.try_subscribe() {
            tracing::error!("Queue already subscribed");
            return Err(PubSubError::QueueAlreadySubscribed(queue_name.to_string()));
        }

        let sns_arn = queue.sns_arn().to_string();
        let sqs_arn = queue.arn().to_string();
        let sqs_url = queue.url().to_string();

        if let Err(e) = self.ensure_subscription(&sns_arn, &sqs_arn).await {
            tracing::error!(source = %e, "Error ensuring SNS subscription");
            queue.unsubscribe();
            return Err(e);
        }

        let stream = self.poll_sqs_stream(&sqs_url, queue_settings);

        tokio::pin!(stream);

        let mut loop_result: Result<(), PubSubError> = Ok(());

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Subscription ended for the topic");
                    break;
                }
                maybe_msg = stream.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => {
                            tracing::info!("Got new message");
                            let mut all_handled = true;
                            let msg = Arc::new(msg);
                            let mut join_set: JoinSet<Result<(), HandlerError>> = JoinSet::new();
                            let mut name_by_id: HashMap<tokio::task::Id, &'static str> =
                                HashMap::new();
                            let mut cancelled = false;
                            let mut handler_iter = handlers.iter();

                            let initial_count = match handler_execution_mode {
                                HandlerExecutionMode::Sequential => 1,
                                HandlerExecutionMode::Parallel => handlers.len(),
                            };
                            for _ in 0..initial_count {
                                if let Some(handler) = handler_iter.next() {
                                    let h = handler.clone();
                                    let m = msg.clone();
                                    let name = h.handler_name();
                                    let abort_handle = join_set
                                        .spawn(async move { h.handle(&m).await });
                                    name_by_id.insert(abort_handle.id(), name);
                                }
                            }

                            while !join_set.is_empty() {
                                let join_result = tokio::select! {
                                    biased;
                                    _ = cancellation_token.cancelled() => {
                                        join_set.shutdown().await;
                                        cancelled = true;
                                        break;
                                    }
                                    r = join_set.join_next() => r.expect("non-empty join set"),
                                };

                                match join_result {
                                    Ok(Ok(())) => {}
                                    Ok(Err(HandlerError::ProcessedAlready { message, handler })) => {
                                        tracing::warn!(msg_len = message.len(), handler = %handler, "Skipped the message in handler");
                                    }
                                    Ok(Err(HandlerError::ProcessingMessage { message, handler, source })) => {
                                        all_handled = false;
                                        tracing::error!(msg_len = message.len(), handler = %handler, source = %source, "Failed to process the message in the handler");
                                    }
                                    Err(join_err) => {
                                        all_handled = false;
                                        let handler_name = name_by_id
                                            .get(&join_err.id())
                                            .copied()
                                            .unwrap_or("<unknown>");
                                        if join_err.is_panic() {
                                            tracing::error!(handler = handler_name, "Handler panicked; treating message as not handled");
                                        } else if join_err.is_cancelled() {
                                            tracing::warn!(handler = handler_name, "Handler aborted");
                                        } else {
                                            tracing::error!(handler = handler_name, error = %join_err, "Handler task failed");
                                        }
                                    }
                                }

                                if matches!(handler_execution_mode, HandlerExecutionMode::Sequential) {
                                    if let Some(handler) = handler_iter.next() {
                                        let h = handler.clone();
                                        let m = msg.clone();
                                        let name = h.handler_name();
                                        let abort_handle = join_set
                                            .spawn(async move { h.handle(&m).await });
                                        name_by_id.insert(abort_handle.id(), name);
                                    }
                                }
                            }

                            if cancelled {
                                tracing::info!("Cancelled during message processing; skipping delete");
                                break;
                            }

                            if delete_mode == MessageDeleteMode::DeleteAllCalled
                                || (delete_mode == MessageDeleteMode::DeleteAllHandled && all_handled)
                            {
                                if let Err(e) = msg.delete(&self.sqs_client, &sqs_url).await {
                                    tracing::error!(
                                        error = %e,
                                        queue = %sqs_url,
                                        "Failed to delete message; will redeliver after visibility timeout"
                                    );
                                } else {
                                    tracing::info!(msg_len = msg.body().len(), "Message deleted");
                                }
                            }
                        }
                        Some(Err(PubSubError::PollingQueue { queue, source })) => {
                            tracing::error!(%queue, source = %source, "Polling queue retries exhausted; will retry on next poll");
                        }
                        Some(Err(err)) => {
                            tracing::error!(error = %err, "Unexpected error from poll stream");
                        }
                        None => {
                            loop_result = Err(PubSubError::UnexpectedCompletion(sqs_url.clone()));
                            break;
                        }
                    }
                }
            }
        }

        queue.unsubscribe();

        loop_result
    }

    fn poll_sqs_stream(
        &self,
        sqs_url: &str,
        queue_settings: QueueSettings,
    ) -> impl Stream<Item = Result<IncomingMessage, PubSubError>> {
        stream::unfold(VecDeque::new(), move |mut buffer| {
            let qs = queue_settings.clone();
            async move {
                if let Some(msg) = buffer.pop_front() {
                    return Some((Ok(msg), buffer));
                }

                loop {
                    let retry_strategy =
                        ExponentialBackoff::from_millis(qs.retry_interval_ms).take(qs.retry_count);

                    match Retry::spawn(retry_strategy, || async {
                        self.sqs_client
                            .receive_message()
                            .queue_url(sqs_url)
                            .max_number_of_messages(qs.max_message_count as i32)
                            .wait_time_seconds(qs.wait_time_seconds)
                            .send()
                            .await
                    })
                    .await
                    {
                        Ok(resp) => {
                            if let Some(messages) = resp.messages {
                                let count = messages.len();
                                tracing::debug!(count, "Messages pulled from the queue");
                                for msg in messages {
                                    if let Some((body, receipt_handle)) =
                                        msg.body.zip(msg.receipt_handle)
                                    {
                                        let incoming_message =
                                            IncomingMessage::new(body, receipt_handle);
                                        buffer.push_back(incoming_message);
                                    }
                                }
                            }

                            if let Some(msg) = buffer.pop_front() {
                                return Some((Ok(msg), buffer));
                            }
                        }
                        Err(e) => {
                            return Some((
                                Err(PubSubError::PollingQueue {
                                    queue: sqs_url.to_string(),
                                    source: e,
                                }),
                                buffer,
                            ));
                        }
                    }
                }
            }
        })
    }

    fn queue_settings(&self, context: &'static str) -> Result<QueueSettings, PubSubError> {
        self.queue_settings
            .lock()
            .map_err(|_| PubSubError::LockPoisoned {
                resource: "queue_settings",
            })?
            .clone()
            .ok_or(PubSubError::QueueSettings { context })
    }

    fn topic_settings(&self) -> Result<Option<TopicSettings>, PubSubError> {
        Ok(self
            .topic_settings
            .lock()
            .map_err(|_| PubSubError::LockPoisoned {
                resource: "topic_settings",
            })?
            .clone())
    }
}
