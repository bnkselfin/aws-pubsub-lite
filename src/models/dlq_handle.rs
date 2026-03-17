use crate::{
    errors::ResourceErrors,
    models::{ResourceName, ResourceType},
};

#[derive(Debug)]
pub struct DlqHandle {
    name: String,
    url: String,
    arn: String,
}

impl DlqHandle {
    pub fn new(queue_name: ResourceName, url: String, arn: String) -> Result<Self, ResourceErrors> {
        queue_name.validate_resource_type(ResourceType::Queue)?;

        Ok(Self {
            name: queue_name.to_string(),
            url,
            arn,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn arn(&self) -> &str {
        &self.arn
    }
}
