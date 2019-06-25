use sha1::Sha1;

pub struct Field {
    pub name: String,
    pub hash: String,
}

impl Field {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            hash: Sha1::from(name.as_bytes()).digest().to_string(),
        }
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
