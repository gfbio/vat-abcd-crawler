use std::fmt;
use std::borrow::Cow;

/// This enum represents the VAT data types.
#[derive(Clone, Debug)]
pub enum VatType {
    Textual(String),
    Numeric(f64),
}

impl From<String> for VatType {
    fn from(value: String) -> Self {
        VatType::Textual(value)
    }
}

impl From<&str> for VatType {
    fn from(value: &str) -> Self {
        VatType::Textual(value.into())
    }
}

impl<'a> From<Cow<'a, str>> for VatType {
    fn from(value: Cow<'a, str>) -> Self {
        VatType::Textual(value.into())
    }
}

impl From<f64> for VatType {
    fn from(value: f64) -> Self {
        VatType::Numeric(value)
    }
}

impl fmt::Display for VatType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VatType::Textual(value) => write!(f, "{}", value),
            VatType::Numeric(value) => write!(f, "{}", value),
        }
    }
}
