use std::fmt::Display;

#[derive(Debug, Clone, Copy)]
pub enum SeparatorSymbol {
    Underscore,
    Hyphen,
}

impl Display for SeparatorSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SeparatorSymbol::Underscore => '_',
                SeparatorSymbol::Hyphen => '-',
            }
        )
    }
}
