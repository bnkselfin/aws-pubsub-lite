use thiserror::Error;

#[derive(Error, Debug)]
pub enum ResourceNameError {
    #[error("Resource name is empty")]
    Empty,
    #[error("Resource name '{name}' exceeds {max} characters")]
    TooLong { name: String, max: usize },
    #[error("Resource name '{name}' contains invalid characters (must match [a-zA-Z0-9_-]+)")]
    InvalidPattern { name: String },
}
