use crate::settings::AwsSettings;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;

pub async fn new_sdk_config(aws_settings: &AwsSettings) -> anyhow::Result<SdkConfig> {
    let credentials = Credentials::new(
        aws_settings.access_key_id.clone(),
        aws_settings.secret_access_key.clone(),
        None,
        None,
        "static",
    );

    Ok(aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(aws_settings.region.clone()))
        .credentials_provider(credentials)
        .load()
        .await)
}

pub async fn new_sdk_config_from_env() -> anyhow::Result<SdkConfig> {
    Ok(aws_config::defaults(BehaviorVersion::latest()).load().await)
}
