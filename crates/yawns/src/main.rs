use crate::prelude::*;
use clap::Parser;

mod error;
mod kms;
mod prelude;

#[derive(Debug, clap::Parser)]
#[command(
    author,
    version,
    about,
    long_about = "Shortcuts for commonly used AWS commands"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: SubCommands,
}

#[derive(Debug, clap::Parser)]
pub enum SubCommands {
    /// AWS KMS (AWS Key Management Service)
    KMS(crate::kms::Cli),
}

fn main() -> Result<()> {
    env_logger::init();
    color_eyre::install()?;

    let cli = Cli::parse();

    match cli.command {
        SubCommands::KMS(cli) => crate::kms::run(cli),
    }
    .map_err(|err| eyre!(err))
}
