use thiserror::Error;

use crate::models::ResourceType;

#[derive(Error, Debug)]
pub enum ResourceErrors {
    #[error(
        "Invalid resource type '{invalid_type}' provided for '{name}' with type '{required_type}'"
    )]
    InvalidResourceType {
        name: String,
        required_type: ResourceType,
        invalid_type: ResourceType,
    },
}
