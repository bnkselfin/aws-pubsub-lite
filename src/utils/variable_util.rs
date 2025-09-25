use std::env;

use anyhow::Context;

pub fn get_var(key: &str) -> anyhow::Result<String> {
    env::var(key).context(format!("Variable: '{key}' is missing"))
}
