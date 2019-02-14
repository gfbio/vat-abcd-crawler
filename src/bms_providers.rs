use failure::Error;
use std::collections::HashMap;
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

/// This function downloads a list of providers from the BMS.
pub fn load_bms_providers(url: &str) -> Result<Vec<BmsProvider>, Error> {
    Ok(
        reqwest::Client::new()
            .get(url)
            .send()?
            .json()?
    )
}

/// This function downloads the BMS providers and provides them
/// as a map from `url`to `BmsProvider`.
pub fn load_bms_providers_as_map(url: &str) -> Result<HashMap<String, BmsProvider>, Error> {
    let providers = load_bms_providers(url)?;
    Ok(
        providers.into_iter()
            .map(|provider| (provider.url.clone(), provider))
            .collect()
    )
}
