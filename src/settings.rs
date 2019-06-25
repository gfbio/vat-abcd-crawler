use std::path::Path;

use config::Config;
use config::ConfigError;
use config::File;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct GeneralSettings {
    pub log_file: String,
    pub debug: bool,
}

#[derive(Debug, Deserialize)]
pub struct AbcdSettings {
    pub fields_file: String,
    pub landing_page_field: String,
}

#[derive(Debug, Deserialize)]
pub struct PangaeaSettings {
    pub search_url: String,
    pub scroll_url: String,
}

#[derive(Debug, Deserialize)]
pub struct TerminologyServiceSettings {
    pub landingpage_url: String,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseSettings {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub database: String,
    pub user: String,
    pub password: String,
    pub schema: String,
    pub dataset_table: String,
    pub listing_view: String,
    pub temp_dataset_table: String,
    pub surrogate_key_column: String,
    pub dataset_id_column: String,
    pub dataset_path_column: String,
    pub dataset_landing_page_column: String,
    pub dataset_provider_column: String,
    pub unit_table: String,
    pub temp_unit_table: String,
    pub unit_indexed_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct DebugSettings {
    pub dataset_start: Option<usize>,
    pub dataset_limit: Option<usize>,
}

/// This struct stores the program settings.
#[derive(Debug, Deserialize)]
pub struct Settings {
    pub abcd: AbcdSettings,
    pub pangaea: PangaeaSettings,
    pub terminology_service: TerminologyServiceSettings,
    pub database: DatabaseSettings,
    pub debug: DebugSettings,
    pub general: GeneralSettings,
}

impl Settings {
    pub fn new(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::from(Path::new("settings-default.toml")))?;
        s.merge(File::from(Path::new("settings.toml")))?;
        if let Some(path) = path {
            s.merge(File::from(path))?;
        }

        s.try_into()
    }
}

#[cfg(test)]
mod test {
    use crate::test_utils;

    use super::*;

    #[test]
    fn load_file() {
        let path = test_utils::create_temp_file_with_suffix(
            ".toml",
            r#"
            [general]
            debug = true
            "#,
        );

        let settings = Settings::new(Some(&path)).expect("Unable to load settings.");

        assert!(settings.general.debug);
    }
}
