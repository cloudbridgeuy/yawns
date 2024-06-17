pub use crate::error::Error;

pub use color_eyre::eyre::Result;

// Generic Wrapper tuple struct for newtype pattern.
pub struct W<T>(pub T);
