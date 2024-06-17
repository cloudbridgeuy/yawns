use crate::prelude::*;

#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Get the policy attached to the key.
    #[clap(name = "get-policy")]
    GetPolicy(GetPolicyOptions),
}

#[derive(Debug, clap::Args, serde::Serialize, serde::Deserialize, Clone)]
pub struct GetPolicyOptions {
    /// AWS KMS Key name.
    key: String,
}

#[derive(Debug, clap::Parser)]
#[command(name = "kms")]
#[command(about = "AWS KMS (AWS Key Management Serivice)")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

pub fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::GetPolicy(options) => get_policy(options),
    }
}

pub fn get_policy(_options: GetPolicyOptions) -> Result<()> {
    todo!()
}
