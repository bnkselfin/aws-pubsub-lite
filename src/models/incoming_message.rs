use crate::errors::PubSubError;

#[derive(Debug)]
pub struct IncomingMessage {
    body: String,
    receipt_handle: String,
}

impl IncomingMessage {
    pub(crate) fn new(body: String, receipt_handle: String) -> IncomingMessage {
        IncomingMessage {
            body,
            receipt_handle,
        }
    }

    pub fn body(&self) -> &str {
        &self.body
    }

    pub fn receipt_handle(&self) -> &str {
        &self.receipt_handle
    }

    pub(crate) async fn delete(
        &self,
        client: &aws_sdk_sqs::Client,
        queue_url: &str,
    ) -> Result<(), PubSubError> {
        client
            .delete_message()
            .queue_url(queue_url)
            .receipt_handle(&self.receipt_handle)
            .send()
            .await
            .map_err(|e| PubSubError::DeleteMessage {
                message: self.body.to_string(),
                queue_url: queue_url.to_string(),
                source: e,
            })?;
        Ok(())
    }
}
