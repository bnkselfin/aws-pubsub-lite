use std::sync::atomic::{AtomicBool, Ordering};

use crate::{
    errors::ResourceErrors,
    models::{ResourceName, ResourceType},
};

#[derive(Debug)]
pub struct Queue {
    name: String,
    arn: String,
    url: String,
    sns_arn: String,
    subscribed: AtomicBool,
}

impl Queue {
    pub fn new(
        queue_name: ResourceName,
        arn: &str,
        url: String,
        sns_arn: &str,
    ) -> Result<Queue, ResourceErrors> {
        queue_name.validate_resource_type(ResourceType::Queue)?;

        Ok(Self {
            name: queue_name.to_string(),
            arn: arn.to_string(),
            url: url.to_string(),
            sns_arn: sns_arn.to_string(),
            subscribed: AtomicBool::new(false),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn arn(&self) -> &str {
        &self.arn
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn sns_arn(&self) -> &str {
        &self.sns_arn
    }

    pub fn is_subscribed(&self) -> bool {
        self.subscribed.load(Ordering::Acquire)
    }

    pub(crate) fn try_subscribe(&self) -> bool {
        self.subscribed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn unsubscribe(&self) {
        self.subscribed.store(false, Ordering::Release);
    }
}
