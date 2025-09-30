use aws_sdk_sns::operation::create_topic::CreateTopicError;
use aws_sdk_sns::operation::list_subscriptions_by_topic::ListSubscriptionsByTopicError;
use aws_sdk_sns::operation::publish::PublishError;
use aws_sdk_sns::operation::set_topic_attributes::SetTopicAttributesError;
use aws_sdk_sns::operation::subscribe::SubscribeError;
use aws_sdk_sqs::operation::create_queue::CreateQueueError;
use aws_sdk_sqs::operation::delete_message::DeleteMessageError;
use aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesError;
use aws_sdk_sqs::operation::receive_message::ReceiveMessageError;
use aws_sdk_sqs::operation::set_queue_attributes::SetQueueAttributesError;
use aws_sdk_sqs::types::QueueAttributeName;
use thiserror::Error;

use crate::errors::ResourceErrors;
use crate::models::SnsTopicAttribute;

#[derive(Error, Debug)]
pub enum PubSubError {
    #[error("Topic '{0}' not exists")]
    TopicNotExists(String),
    #[error("Queue '{0}' not exists")]
    QueueNotExists(String),
    #[error("Error polling queue '{queue}': {source}")]
    PollingQueue {
        queue: String,
        #[source]
        source: aws_sdk_sqs::error::SdkError<ReceiveMessageError>,
    },
    #[error(transparent)]
    Resource(#[from] ResourceErrors),
    #[error("Error creating sns topic '{topic}': {source}")]
    SNSTopicCreation {
        topic: String,
        #[source]
        source: aws_sdk_sns::error::SdkError<CreateTopicError>,
    },
    #[error("Error getting SNS topic '{0}' arn")]
    GettingSNSTopicArn(String),
    #[error("Error creating sqs queue '{queue}' for sns topic(arn) '{sns_arn}': {source}")]
    SQSQueueCreation {
        sns_arn: String,
        queue: String,
        #[source]
        source: aws_sdk_sns::error::SdkError<CreateQueueError>,
    },
    #[error("Error getting SQS queue '{0}' url")]
    GettingSQSQueueUrl(String),
    #[error("Error getting attribute '{attribute}' of SQS queue '{queue}': {source}")]
    GettingQueueAttributes {
        queue: String,
        attribute: QueueAttributeName,
        #[source]
        source: aws_sdk_sqs::error::SdkError<GetQueueAttributesError>,
    },
    #[error("Error setting attribute '{attribute}' of SQS queue '{queue_arn}': {source}")]
    SettingQueueAttributes {
        queue_arn: String,
        attribute: QueueAttributeName,
        #[source]
        source: aws_sdk_sqs::error::SdkError<SetQueueAttributesError>,
    },
    #[error("No attributes found for SQS queue '{0}'")]
    NoQueueAttributes(String),
    #[error("Arn was not found for SQS queue '{0}'")]
    QueueArnNotFound(String),
    #[error("Empty message")]
    EmptyMessage,
    #[error("Error pushing message (len={}) to topic '{topic}': {source}", message.len())]
    PublishMessage {
        topic: String,
        message: String,
        #[source]
        source: aws_sdk_sns::error::SdkError<PublishError>,
    },
    #[error("Queue '{0}' already subscribed")]
    QueueAlreadySubscribed(String),
    #[error("Error subscribing queue(arn) '{queue_arn}' to topic '{topic}': {source}")]
    SubscribingQueue {
        topic: String,
        queue_arn: String,
        #[source]
        source: aws_sdk_sns::error::SdkError<SubscribeError>,
    },
    #[error("Error listing subscriptions for SNS topic '{topic}': {source}")]
    ListingSubscriptions {
        topic: String,
        #[source]
        source: aws_sdk_sns::error::SdkError<ListSubscriptionsByTopicError>,
    },
    #[error("Error deleting message (len={}) in queue(url) '{queue_url}'", message.len())]
    DeleteMessage {
        message: String,
        queue_url: String,
        #[source]
        source: aws_sdk_sqs::error::SdkError<DeleteMessageError>,
    },
    #[error("Poll stream for queue '{0}' completed unexpectedly")]
    UnexpectedCompletion(String),
    #[error("Queue settings are not set (required for {context})")]
    QueueSettings { context: &'static str },
    #[error("Lock for '{resource}' is poisoned")]
    LockPoisoned { resource: &'static str },
    #[error("Error setting attribute '{attribute}' of SNS topic '{topic_arn}': {source}")]
    SettingTopicAttributes {
        topic_arn: String,
        attribute: SnsTopicAttribute,
        #[source]
        source: aws_sdk_sns::error::SdkError<SetTopicAttributesError>,
    },
}
