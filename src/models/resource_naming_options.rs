use crate::models::SeparatorSymbol;

#[derive(Debug, Clone)]
pub struct ResourceNamingOptions {
    prefix: Option<String>,
    suffix: Option<String>,
    prefix_separator: Option<SeparatorSymbol>,
    suffix_separator: Option<SeparatorSymbol>,
}

impl ResourceNamingOptions {
    pub fn new(
        prefix: Option<&str>,
        suffix: Option<&str>,
        prefix_separator: Option<SeparatorSymbol>,
        suffix_separator: Option<SeparatorSymbol>,
    ) -> Self {
        Self {
            prefix: prefix.map(|p| p.to_string()),
            suffix: suffix.map(|s| s.to_string()),
            prefix_separator,
            suffix_separator,
        }
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub fn suffix(&self) -> Option<&str> {
        self.suffix.as_deref()
    }

    pub fn prefix_separator(&self) -> Option<&SeparatorSymbol> {
        self.prefix_separator.as_ref()
    }

    pub fn suffix_separator(&self) -> Option<&SeparatorSymbol> {
        self.suffix_separator.as_ref()
    }
}
