use std::str::FromStr;

use anyhow::{Result, anyhow};

#[derive(Debug, Clone, Copy)]
pub enum BackoffFunction {
    Linear,
    Arithmetic,
    Geometric,
    Exponential,
}

impl BackoffFunction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::Arithmetic => "arithmetic",
            Self::Geometric => "geometric",
            Self::Exponential => "exponential",
        }
    }
}

impl FromStr for BackoffFunction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "linear" => Ok(Self::Linear),
            "arithmetic" => Ok(Self::Arithmetic),
            "geometric" => Ok(Self::Geometric),
            "exponential" => Ok(Self::Exponential),
            other => Err(anyhow!(
                "Unknown backoff function '{other}' (expected linear|arithmetic|geometric|exponential)"
            )),
        }
    }
}
