use crate::prelude::*;

pub async fn get_sdk_config_from_global(global: crate::Global) -> Result<aws_config::SdkConfig> {
    let config_loader = aws_config::from_env();

    let config_loader = if let Some(region) = global.region.clone() {
        config_loader.region(aws_types::region::Region::new(region.clone()))
    } else {
        config_loader
    };

    let config_loader = if let Some(profile_name) = global.profile.clone() {
        config_loader.profile_name(profile_name)
    } else {
        config_loader
    };

    Ok(config_loader.load().await)
}
