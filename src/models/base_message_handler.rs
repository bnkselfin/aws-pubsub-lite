use std::future::Future;
use std::pin::Pin;

use crate::errors::HandlerError;
use crate::models::IncomingMessage;

pub trait BaseMessageHandler: Send + Sync + 'static {
    fn handler_name(&self) -> &'static str;

    fn handle(
        &self,
        message: &IncomingMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + '_>>;
}
