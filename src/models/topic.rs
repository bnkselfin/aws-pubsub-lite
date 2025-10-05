use crate::{
    errors::ResourceErrors,
    models::{ResourceName, ResourceType},
};

#[derive(Debug, Clone)]
pub struct Topic {
    name: String,
    arn: String,
}

impl Topic {
    pub fn new(topic_name: ResourceName, arn: &str) -> Result<Self, ResourceErrors> {
        topic_name.validate_resource_type(ResourceType::Topic)?;

        Ok(Self {
            name: topic_name.to_string(),
            arn: arn.to_string(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn arn(&self) -> &str {
        &self.arn
    }
}
