use crate::errors::PubSubError;
use crate::models::Queue;
use crate::models::ResourceName;
use crate::models::SnsTopicAttribute;
use crate::models::Topic;
use crate::settings::{QueueSettings, TopicSettings};
use aws_config::SdkConfig;
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sqs::Client as SqsClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

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
