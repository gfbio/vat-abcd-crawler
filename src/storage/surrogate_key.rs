use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub struct SurrogateKey {
    id_to_key: HashMap<String, u32>,
    next_key: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SurrogateKeyType {
    New(u32),
    Existing(u32),
}

impl SurrogateKey {
    pub fn new() -> Self {
        Self {
            id_to_key: Default::default(),
            next_key: 1,
        }
    }

    pub fn for_id(&mut self, id: &str) -> SurrogateKeyType {
        match self.id_to_key.entry(id.into()) {
            Occupied(entry) => SurrogateKeyType::Existing(*entry.get()),
            Vacant(entry) => {
                let key = *entry.insert(self.next_key);
                self.next_key += 1;

                SurrogateKeyType::New(key)
            }
        }
    }
}

impl Default for SurrogateKey {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_keys() {
        let mut surrogate_key = SurrogateKey::new();

        for i in 1..=5 {
            assert_eq!(
                SurrogateKeyType::New(i),
                surrogate_key.for_id(&i.to_string())
            );
        }
    }

    #[test]
    fn existing_key() {
        let mut surrogate_key = SurrogateKey::new();

        assert_eq!(SurrogateKeyType::New(1), surrogate_key.for_id("foo"));
        assert_eq!(SurrogateKeyType::Existing(1), surrogate_key.for_id("foo"));
        assert_eq!(SurrogateKeyType::New(2), surrogate_key.for_id("bar"));
    }
}
