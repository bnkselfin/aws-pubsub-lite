use crate::errors::PubSubError;
use crate::models::Queue;
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
