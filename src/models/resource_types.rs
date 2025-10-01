use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResourceType {
    Topic,
    Queue,
}

impl Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceType::Topic => write!(f, "Topic"),
            ResourceType::Queue => write!(f, "Queue"),
        }
    }
}
