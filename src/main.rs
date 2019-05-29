use std::fs::File;
use std::path::Path;

use clap::{crate_authors, crate_description, crate_version, App, Arg};
use failure::Error;
use log::{error, info, trace, warn};
use simplelog::{CombinedLogger, SharedLogger, TermLogger, WriteLogger};

use settings::Settings;

use crate::abcd::{AbcdFields, AbcdParser, ArchiveReader};
use crate::bms::{download_datasets, load_bms_datasets, BmsDataset, BmsProviders};
use crate::database_sink::DatabaseSink;

mod abcd;
mod bms;
mod database_sink;
mod settings;
#[cfg(test)]
mod test_utils;
mod vat_type;

fn main() {
    let settings = initialize_settings().expect("Unable to load settings file.");

    initialize_logger(Path::new(&settings.general.log_file), &settings)
        .expect("Unable to initialize logger.");

    let abcd_fields = match AbcdFields::from_path(Path::new(&settings.abcd.fields_file)) {
        Ok(fields) => fields,
        Err(e) => {
            error!("Unable to load ABCD file: {}", e);
            return; // stop program
        }
    };

    let mut database_sink = match DatabaseSink::new(&settings.database, &abcd_fields) {
        Ok(sink) => sink,
        Err(e) => {
            error!("Unable to create database sink: {}", e);
            return; // stop program
        }
    };

    let bms_providers = match BmsProviders::from_url(&settings.bms.provider_url) {
        Ok(providers) => providers,
        Err(e) => {
            error!("Unable to download providers from BMS: {}", e);
            return; // stop program
        }
    };

    let bms_datasets = match load_bms_datasets(&settings.bms.monitor_url) {
        Ok(datasets) => datasets,
        Err(e) => {
            error!("Unable to download datasets from BMS: {}", e);
            return; // stop program
        }
    };

    if let Err(e) = process_datasets(
        &settings,
        &abcd_fields,
        &mut database_sink,
        bms_providers,
        &bms_datasets,
    ) {
        error!("Error processing datasets: {}", e);
    };
}

fn process_datasets(
    settings: &Settings,
    abcd_fields: &AbcdFields,
    database_sink: &mut DatabaseSink,
    bms_providers: BmsProviders,
    bms_datasets: &Vec<BmsDataset>,
) -> Result<(), Error> {
    let temp_dir = tempfile::tempdir()?;

    let mut abcd_parser = AbcdParser::new(&abcd_fields);

    for path_result in download_datasets(temp_dir.path(), &bms_datasets)
        .skip(
            settings
                .debug
                .dataset_start
                .filter(|_| settings.general.debug)
                .unwrap_or(std::usize::MIN),
        )
        .take(
            settings
                .debug
                .dataset_limit
                .filter(|_| settings.general.debug)
                .unwrap_or(std::usize::MAX),
        )
    {
        let download = match path_result {
            Ok(d) => d,
            Err(e) => {
                warn!("Unable to download file: {}", e);
                continue;
            }
        };
        trace!("Temp file: {}", download.path.display());
        info!(
            "Processing `{}` @ `{}` ({})",
            download.dataset.dataset,
            download.dataset.provider_datacenter,
            download
                .dataset
                .get_latest_archive()
                .map(|archive| archive.xml_archive.as_str())
                .unwrap_or_else(|_| "-")
        );

        let bms_provider = match bms_providers.value_of(&download.dataset.provider_url) {
            Some(provider) => provider,
            None => {
                warn!(
                    "Unable to retrieve BMS provider from map for {}",
                    download.dataset.provider_url
                );
                continue;
            }
        };

        let landing_page = match download.dataset.get_landing_page(&settings, &bms_provider) {
            Ok(landing_page) => landing_page,
            Err(e) => {
                warn!(
                    "Unable to generate landing page for {}; {}",
                    download.dataset.dataset, e
                );
                continue;
            }
        };

        for xml_bytes_result in ArchiveReader::from_path(&download.path)
            .unwrap()
            .bytes_iter()
        {
            let xml_bytes = match xml_bytes_result {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!("Unable to read file from zip archive: {}", e);
                    continue;
                }
            };

            let abcd_data = match abcd_parser.parse(
                &download.url,
                &landing_page,
                &bms_provider.name,
                &xml_bytes,
            ) {
                Ok(data) => data,
                Err(e) => {
                    warn!("Unable to retrieve ABCD data: {}", e);
                    continue;
                }
            };

            trace!("{:?}", abcd_data.dataset);

            match database_sink.insert_dataset(&abcd_data) {
                Ok(_) => (),
                Err(e) => warn!("Unable to insert dataset into database: {}", e),
            };
        }
    }

    match database_sink.migrate_schema() {
        Ok(_) => info!("Schema migration complete."),
        Err(e) => warn!("Unable to migrate schema: {}", e),
    };

    Ok(())
}

fn initialize_settings() -> Result<Settings, Error> {
    let matches = App::new("VAT ABCD Crawler")
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .arg(
            Arg::with_name("settings")
                .index(1)
                .short("s")
                .long("settings")
                .value_name("SETTINGS")
                .help("Specify the settings file")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let settings_path = matches.value_of("settings").map(Path::new);

    Ok(Settings::new(settings_path)?)
}

/// Initialize the logger.
fn initialize_logger(file_path: &Path, settings: &Settings) -> Result<(), Error> {
    let mut loggers: Vec<Box<SharedLogger>> = Vec::new();

    let log_level = if settings.general.debug {
        simplelog::LevelFilter::Debug
    } else {
        simplelog::LevelFilter::Info
    };

    if let Some(term_logger) = TermLogger::new(log_level, simplelog::Config::default()) {
        loggers.push(term_logger);
    }

    if let Ok(file) = File::create(file_path) {
        loggers.push(WriteLogger::new(
            log_level,
            simplelog::Config::default(),
            file,
        ));
    }

    CombinedLogger::init(loggers)?;

    Ok(())
}
