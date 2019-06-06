use std::fs::File;
use std::path::Path;

use clap::{crate_authors, crate_description, crate_version, App, Arg};
use failure::Error;
use log::{error, info, trace, warn};
use simplelog::{CombinedLogger, SharedLogger, TermLogger, WriteLogger};

use settings::Settings;

use crate::abcd::{AbcdFields, AbcdParser, ArchiveReader};
use crate::file_downloader::FileDownloader;
use crate::pangaea::{PangaeaSearchResult, PangaeaSearchResultEntry};
use crate::settings::TerminologyServiceSettings;
use crate::storage::DatabaseSink;

mod abcd;
mod file_downloader;
mod pangaea;
mod settings;
mod storage;
#[cfg(test)]
mod test_utils;
mod vat_type;

fn main() -> Result<(), Error> {
    let settings = initialize_settings().expect("Unable to load settings file.");

    initialize_logger(Path::new(&settings.general.log_file), &settings)
        .expect("Unable to initialize logger.");

    let abcd_fields = match AbcdFields::from_path(Path::new(&settings.abcd.fields_file)) {
        Ok(fields) => fields,
        Err(e) => {
            error!("Unable to load ABCD file: {}", e);
            return Err(e); // stop program
        }
    };

    let mut database_sink = match DatabaseSink::new(&settings.database, &abcd_fields) {
        Ok(sink) => sink,
        Err(e) => {
            error!("Unable to create storage sink: {}", e);
            return Err(e); // stop program
        }
    };

    let datasets = match PangaeaSearchResult::retrieve_all_entries(&settings.pangaea) {
        Ok(search_entries) => search_entries,
        Err(e) => {
            error!("Unable to download dataset metadata from Pangaea: {}", e);
            return Err(e); // stop program
        }
    };

    if let Err(e) = process_datasets(&settings, &abcd_fields, &mut database_sink, &datasets) {
        error!("Error processing datasets: {}", e);
    };

    Ok(())
}

fn process_datasets(
    settings: &Settings,
    abcd_fields: &AbcdFields,
    database_sink: &mut DatabaseSink,
    datasets: &[PangaeaSearchResultEntry],
) -> Result<(), Error> {
    let temp_dir = tempfile::tempdir()?;

    let mut abcd_parser = AbcdParser::new(&settings.abcd, &abcd_fields);

    for dataset in datasets
        .iter()
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
        let file_path = temp_dir.path().join(dataset.id()).join(".zip");
        if let Err(e) = FileDownloader::from_url(dataset.download_url()).to_path(&file_path) {
            warn!("Unable to download file: {}", e);
            continue;
        }

        trace!("Temp file: {}", file_path.display());
        info!(
            "Processing `{}` @ `{}` ({})",
            dataset.id(),
            dataset.publisher(),
            dataset.download_url(),
        );

        let landing_page_url: String =
            propose_landing_page(&settings.terminology_service, dataset.download_url());

        for xml_bytes_result in ArchiveReader::from_path(&file_path).unwrap().bytes_iter() {
            let xml_bytes = match xml_bytes_result {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!("Unable to read file from zip archive: {}", e);
                    continue;
                }
            };

            let abcd_data = match abcd_parser.parse(
                dataset.id(),
                dataset.download_url(),
                &landing_page_url,
                &dataset.publisher(),
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
                Err(e) => warn!("Unable to insert dataset into storage: {}", e),
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

fn propose_landing_page(
    terminology_service_settings: &TerminologyServiceSettings,
    dataset_url: &str,
) -> String {
    format!(
        "{base_url}?archive={dataset_url}",
        base_url = terminology_service_settings.landingpage_url,
        dataset_url = dataset_url,
    )
}
