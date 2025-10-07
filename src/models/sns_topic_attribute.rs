use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SnsTopicAttribute {
    DeliveryPolicy,
}

impl SnsTopicAttribute {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeliveryPolicy => "DeliveryPolicy",
        }
    }
}

impl Display for SnsTopicAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
