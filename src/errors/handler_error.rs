use thiserror::Error;

#[derive(Error, Debug)]
pub enum HandlerError {
    #[error("Error processing message (len={}) in handler '{handler}': {source}", message.len())]
    ProcessingMessage {
        message: String,
        handler: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Message (len={}) has already been processed by handler '{handler}'", message.len())]
    ProcessedAlready { message: String, handler: String },
}
