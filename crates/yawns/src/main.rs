use crate::prelude::*;

mod error;
mod prelude;

fn main() -> Result<()> {
    env_logger::init();
    color_eyre::install()?;

    Ok(())
}
