use std::fmt;

use crate::utils::get_var;
use anyhow::Result;

#[derive(Clone)]
pub struct AwsSettings {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl fmt::Debug for AwsSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsSettings")
            .field("region", &self.region)
            .field("access_key_id", &"[REDACTED]")
            .field("secret_access_key", &"[REDACTED]")
            .finish()
    }
}

impl AwsSettings {
    pub fn new(region: &str, access_key_id: &str, secret_access_key: &str) -> Self {
        Self {
            region: region.to_string(),
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
        }
    }

    pub fn from_env() -> Result<Self> {
        let access_key_id = get_var("PUBSUB_AWS_ACCESS_KEY_ID")?;
        let secret_access_key = get_var("PUBSUB_AWS_SECRET_ACCESS_KEY")?;
        let region = get_var("PUBSUB_AWS_REGION")?;

        Ok(Self::new(&region, &access_key_id, &secret_access_key))
    }
}
