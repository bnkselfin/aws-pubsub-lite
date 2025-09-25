mod aws_util;
mod variable_util;

pub use aws_util::new_sdk_config;
pub use aws_util::new_sdk_config_from_env;
pub use variable_util::get_var;
