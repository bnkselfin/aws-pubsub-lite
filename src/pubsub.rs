use crate::errors::PubSubError;
use crate::models::Queue;
use crate::models::ResourceName;
use crate::models::SnsTopicAttribute;
use crate::models::Topic;
use crate::settings::{QueueSettings, TopicSettings};
use aws_config::SdkConfig;
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sqs::{Client as SqsClient, types::QueueAttributeName};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use std::time::Duration;

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
