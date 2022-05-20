use std::fmt::Write;

use sha1::{Digest, Sha1};

pub struct Field {
    pub name: String,
    pub hash: String,
}

impl Field {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            // hash: Sha1::from(name.as_bytes()).digest().to_string(),
            hash: Self::hash_as_hex(name.as_bytes()),
        }
    }

    fn hash_as_hex(bytes: &[u8]) -> String {
        let hash_bytes = Sha1::digest(bytes);
        let hash: &[u8; 20] = hash_bytes.as_ref();

        let mut out = String::with_capacity(40);

        for chunk in hash {
            write!(&mut out, "{:02x}", chunk).expect("cannot fail");
        }

        out
    }
}

impl From<&str> for Field {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for Field {
    fn from(name: String) -> Self {
        Self::new(name.as_str())
    }
}

impl From<&String> for Field {
    fn from(name: &String) -> Self {
        Self::new(name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest() {
        let field = Field::new("test");
        assert_eq!(field.hash, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3");
    }
}
