use std::fmt::{Debug, Display};
use std::sync::LazyLock;

use regex::Regex;

use crate::errors::{ResourceErrors, ResourceNameError};
use crate::models::{ResourceNamingOptions, ResourceType};

static AWS_NAME_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap());

#[derive(Debug, Clone)]
pub struct ResourceName {
    base_name: String,
    resource_type: ResourceType,
    naming_options: ResourceNamingOptions,
}

impl Display for ResourceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(prefix) = self.naming_options.prefix() {
            write!(f, "{}", prefix)?;

            if let Some(separator) = self.naming_options.prefix_separator() {
                write!(f, "{}", separator)?;
            }
        }

        write!(f, "{}", &self.base_name)?;

        if let Some(suffix) = self.naming_options.suffix() {
            if let Some(separator) = self.naming_options.suffix_separator() {
                write!(f, "{}", separator)?;
            }

            write!(f, "{}", suffix)?;
        }

        Ok(())
    }
}

impl ResourceName {
    pub fn new(
        base_name: &str,
        resource_type: ResourceType,
        naming_options: ResourceNamingOptions,
    ) -> Result<Self, ResourceNameError> {
        let resource_name = Self {
            base_name: base_name.to_string(),
            resource_type,
            naming_options,
        };

        let name = resource_name.to_string();

        let max_len = match &resource_name.resource_type {
            ResourceType::Topic => 256,
            ResourceType::Queue => 80,
        };

        Self::validate_aws_resource_name(&name, max_len).map(|_| resource_name)
    }

    fn validate_aws_resource_name(name: &str, max_len: usize) -> Result<(), ResourceNameError> {
        if name.is_empty() {
            return Err(ResourceNameError::Empty);
        }

        if name.len() > max_len {
            return Err(ResourceNameError::TooLong {
                name: name.to_string(),
                max: max_len,
            });
        }

        if !AWS_NAME_PATTERN.is_match(name) {
            return Err(ResourceNameError::InvalidPattern {
                name: name.to_string(),
            });
        }

        Ok(())
    }

    pub fn validate_resource_type(
        &self,
        target_resource_type: ResourceType,
    ) -> Result<(), ResourceErrors> {
        if self.resource_type == target_resource_type {
            Ok(())
        } else {
            Err(ResourceErrors::InvalidResourceType {
                name: self.to_string(),
                required_type: target_resource_type,
                invalid_type: self.resource_type.clone(),
            })
        }
    }
}
