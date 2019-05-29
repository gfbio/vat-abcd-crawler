use std::collections::HashMap;

use failure::Error;
use serde::Deserialize;

/// This struct contains all provider information.
/// The identifier is the `url`, strange as it seems.
#[derive(Debug, Deserialize)]
pub struct BmsProvider {
    pub id: String,
    pub shortname: String,
    pub name: String,
    pub url: String,
    pub biocase_url: String,
}

#[derive(Debug)]
pub struct BmsProviders {
    providers: HashMap<String, BmsProvider>,
}

impl BmsProviders {
    pub fn from_url(url: &str) -> Result<Self, Error> {
        let providers: Vec<BmsProvider> = reqwest::Client::new().get(url).send()?.json()?;
        let provider_map = providers
            .into_iter()
            .map(|provider| (provider.url.clone(), provider))
            .collect();
        Ok(Self {
            providers: provider_map,
        })
    }

    pub fn value_of(&self, url: &str) -> Option<&BmsProvider> {
        self.providers.get(url)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils;

    use super::*;

    #[test]
    fn downloads_providers() {
        let _webserver = test_utils::create_json_webserver(r#"
            [
                {
                    "id": "6",
                    "shortname": "BGBM",
                    "name": "Botanic Garden and Botanical Museum Berlin, Freie Universit\u00e4t Berlin",
                    "url": "www.bgbm.org",
                    "biocase_url": "https:\/\/ww3.bgbm.org\/biocase\/"
                },
                {
                    "id": "5",
                    "shortname": "DSMZ",
                    "name": "Leibniz Institute DSMZ \u2013 German Collection of Microorganisms and Cell Cultures, Braunschweig",
                    "url": "www.dsmz.de",
                    "biocase_url": "http:\/\/biocase.dsmz.de\/wrappers\/biocase"
                }
            ]"#
        );

        let bms_providers = match BmsProviders::from_url(&test_utils::webserver_url()) {
            Ok(providers) => providers,
            Err(error) => panic!(error),
        };

        let bgbm = bms_providers.value_of("www.bgbm.org");
        assert!(bgbm.is_some());
        assert_eq!(bgbm.unwrap().id, "6");

        let dsmz = bms_providers.value_of("www.dsmz.de");
        assert!(dsmz.is_some());
        assert_eq!(dsmz.unwrap().id, "5");

        assert!(bms_providers.value_of("").is_none());
    }

}