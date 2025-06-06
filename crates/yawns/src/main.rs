#![allow(unused)]

use crate::prelude::*;
use clap::Parser;

mod aws;
mod error;
mod kms;
mod prelude;
mod s3;

#[derive(Debug, clap::Parser)]
#[command(
    author,
    version,
    about,
    long_about = "Shortcuts for commonly used AWS commands"
)]
pub struct App {
    #[command(subcommand)]
    pub command: SubCommands,

    #[clap(flatten)]
    global: Global,
}

#[derive(Debug, clap::Args)]
pub struct Global {
    /// AWS Region
    #[clap(long, env = "AWS_REGION", global = true, default_value = "us-east-1")]
    region: Option<String>,
    /// AWS Profile
    #[clap(long, env = "AWS_PROFILE", global = true, default_value = "default")]
    profile: Option<String>,

    /// Whether to display additional information.
    #[clap(long, env = "YAWNS_VERBOSE", global = true, default_value = "false")]
    verbose: bool,
}

#[derive(Debug, clap::Parser)]
pub enum SubCommands {
    /// AWS KMS (AWS Key Management Service)
    KMS(crate::kms::App),

    /// AWS S3
    S3(crate::s3::App),
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    color_eyre::install()?;

    let app = App::parse();

    match app.command {
        SubCommands::KMS(sub_app) => crate::kms::run(sub_app, app.global).await,
        SubCommands::S3(sub_app) => crate::s3::run(sub_app, app.global).await,
    }
    .map_err(|err: color_eyre::eyre::Report| eyre!(err))
}
