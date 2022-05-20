use std::path::Path;

use config::builder::DefaultState;
use config::ConfigBuilder;
use config::ConfigError;
use config::File;
use config::FileFormat;
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
    pub storage_dir: String,
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
        let mut s = ConfigBuilder::<DefaultState>::default();
        s = s.add_source(File::new("settings-default.toml", FileFormat::Toml));
        s = s.add_source(File::new("settings.toml", FileFormat::Toml));
        if let Some(path) = path {
            s = s.add_source(File::from(path));
        }

        let config = s.build()?;

        config.try_deserialize()
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
