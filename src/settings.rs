use config::Config;
use config::File;
use config::ConfigError;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct General {
    pub log_file: String,
}

#[derive(Debug, Deserialize)]
pub struct Abcd {
    pub fields_file: String,
}

#[derive(Debug, Deserialize)]
pub struct Bms {
    pub monitor_url: String,
}

#[derive(Debug, Deserialize)]
pub struct Database {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub database: String,
    pub user: String,
    pub password: String,
    pub schema: String,
    pub dataset_table: String,
    pub temp_dataset_table: String,
    pub dataset_id_column: String,
    pub unit_table: String,
    pub temp_unit_table: String,
    pub unit_indexed_columns: Vec<String>,
}

/// This struct stores the program settings.
#[derive(Debug, Deserialize)]
pub struct Settings {
    pub abcd: Abcd,
    pub bms: Bms,
    pub database: Database,
    pub general: General,
}

impl Settings {
    pub fn new(path: &Path) -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::from(path))?;

        s.try_into()
    }
}