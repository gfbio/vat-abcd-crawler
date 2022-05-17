use std::fs::File;
use std::path::{Path, PathBuf};

use clap::{crate_authors, crate_description, crate_version, App, Arg};
use failure::Error;
use log::{error, info, trace, warn};
use pangaea::PangaeaSearchResult;
use simplelog::{CombinedLogger, SharedLogger, TermLogger, WriteLogger};

use settings::Settings;

use crate::abcd::{AbcdFields, AbcdParser, ArchiveReader};
use crate::file_downloader::FileDownloader;
use crate::pangaea::PangaeaSearchResultEntry;
use crate::settings::TerminologyServiceSettings;
use crate::storage::DatabaseSink;
use crate::vat_type::VatType;

mod abcd;
mod file_downloader;
mod pangaea;
mod settings;
mod storage;
#[cfg(test)]
mod test_utils;
mod vat_type;

#[derive(Debug)]
pub enum Cmd {
    All,
    FileToCsv(PathBuf),
}

fn main() -> Result<(), Error> {
    let (cmd, settings) = initialize_settings().expect("Unable to load settings file.");

    initialize_logger(Path::new(&settings.general.log_file), &settings)
        .expect("Unable to initialize logger.");

    let abcd_fields = match AbcdFields::from_path(Path::new(&settings.abcd.fields_file)) {
        Ok(fields) => fields,
        Err(e) => {
            error!("Unable to load ABCD file: {}", e);
            return Err(e); // stop program
        }
    };

    match cmd {
        Cmd::All => main_all(&settings, &abcd_fields),
        Cmd::FileToCsv(file) => main_single_file(&settings, &abcd_fields, &file),
    }
}

fn main_single_file(
    settings: &Settings,
    abcd_fields: &AbcdFields,
    file: &Path,
) -> Result<(), Error> {
    let mut archive_reader = ArchiveReader::from_path(file)?;

    let mut abcd_parser = AbcdParser::new(&settings.abcd, abcd_fields);

    let mut writer = csv::WriterBuilder::new()
        .delimiter(b',')
        .has_headers(true)
        .from_writer(std::io::stdout());

    // header
    let field_names: Vec<&str> = abcd_fields
        .into_iter()
        .map(|field| field.name.as_str())
        .collect();
    writer.write_record(&field_names)?;

    for xml_bytes_result in archive_reader.bytes_iter() {
        let xml_bytes = match xml_bytes_result {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!("Unable to read file from zip archive: {}", e);
                continue;
            }
        };

        let abcd_data = match abcd_parser.parse("", &file.to_string_lossy(), "", "", &xml_bytes) {
            Ok(data) => data,
            Err(e) => {
                warn!("Unable to retrieve ABCD data: {}", e);
                continue;
            }
        };

        trace!("{:?}", abcd_data.dataset);

        for row in abcd_data.units {
            for field in abcd_fields {
                match row.get(&field.name) {
                    Some(VatType::Textual(value)) => {
                        writer.write_field(value)?;
                    }
                    Some(VatType::Numeric(value)) => {
                        writer.write_field(value.to_string())?;
                    }
                    None => {
                        writer.write_field(&[])?;
                    }
                }
            }

            // finish line
            writer.write_record(None::<&[u8]>)?;
        }
    }

    Ok(())
}

fn main_all(settings: &Settings, abcd_fields: &AbcdFields) -> Result<(), Error> {
    let mut database_sink = match DatabaseSink::new(&settings.database, abcd_fields) {
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

    if let Err(e) = process_datasets(settings, abcd_fields, &mut database_sink, &datasets) {
        error!("Error processing datasets: {}", e);
    };

    Ok(())
}

fn process_datasets(
    settings: &Settings,
    abcd_fields: &AbcdFields,
    database_sink: &mut DatabaseSink<'_>,
    datasets: &[PangaeaSearchResultEntry],
) -> Result<(), Error> {
    let temp_dir = tempfile::tempdir()?;
    let storage_dir = Path::new(&settings.abcd.storage_dir);

    create_or_check_for_directory(&storage_dir);

    let mut abcd_parser = AbcdParser::new(&settings.abcd, abcd_fields);

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
        let file_name = dataset
            .id()
            .chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '-' => c,
                _ => '_',
            })
            .collect::<String>();
        let temp_file_path = temp_dir.path().join(&file_name).with_extension("zip");
        let storage_file_path = storage_dir.join(&file_name).with_extension("zip");

        if let Err(e) = FileDownloader::from_url(dataset.download_url()).to_path(&temp_file_path) {
            warn!(
                "Unable to download file {url} to {path}: {error}",
                url = dataset.download_url(),
                path = temp_file_path.display(),
                error = e,
            );

            let recovery_file_path = storage_file_path.as_path();
            match std::fs::copy(recovery_file_path, &temp_file_path) {
                Ok(_) => info!("Recovered file {file}", file = file_name),
                Err(e) => {
                    warn!(
                        "Recovery of file {file} failed: {error}",
                        file = file_name,
                        error = e,
                    );

                    continue; // skip processing this dataset
                }
            };
        }

        trace!("Temp file: {}", temp_file_path.display());
        info!(
            "Processing `{}` @ `{}` ({})",
            dataset.id(),
            dataset.publisher(),
            dataset.download_url(),
        );

        let landing_page_url: String =
            propose_landing_page(&settings.terminology_service, dataset.download_url());

        let mut archive_reader = match ArchiveReader::from_path(&temp_file_path) {
            Ok(reader) => reader,
            Err(e) => {
                warn!("Unable to read dataset archive: {}", e);
                continue;
            }
        };

        let mut all_inserts_successful = true;

        for xml_bytes_result in archive_reader.bytes_iter() {
            let xml_bytes = match xml_bytes_result {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!("Unable to read file from zip archive: {}", e);
                    all_inserts_successful = false;
                    continue;
                }
            };

            let abcd_data = match abcd_parser.parse(
                dataset.id(),
                dataset.download_url(),
                &landing_page_url,
                dataset.publisher(),
                &xml_bytes,
            ) {
                Ok(data) => data,
                Err(e) => {
                    warn!("Unable to retrieve ABCD data: {}", e);
                    all_inserts_successful = false;
                    continue;
                }
            };

            trace!("{:?}", abcd_data.dataset);

            match database_sink.insert_dataset(&abcd_data) {
                Ok(_) => (),
                Err(e) => {
                    warn!("Unable to insert dataset into storage: {}", e);
                    all_inserts_successful = false;
                }
            };
        }

        if all_inserts_successful && archive_reader.len() > 0 {
            if let Err(e) = std::fs::copy(&temp_file_path, storage_file_path) {
                warn!("Unable to store ABCD file: {}", e);
            }
        }
    }

    match database_sink.migrate_schema() {
        Ok(_) => info!("Schema migration complete."),
        Err(e) => warn!("Unable to migrate schema: {}", e),
    };

    Ok(())
}

fn create_or_check_for_directory(storage_dir: &&Path) {
    if storage_dir.exists() {
        assert!(
            storage_dir.is_dir(),
            "ABCD storage directory path is not a directory",
        );
    } else {
        std::fs::create_dir(&storage_dir).expect("ABCD storage directory is not creatable");
    }
}

fn initialize_settings() -> Result<(Cmd, Settings), Error> {
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
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("file-to-csv")
                .long("file-to-csv")
                .value_name("FILE_TO_CSV")
                .help("Specify a single file archive (zipped ABCD XMLs) that is converted to CSV")
                .required(false)
                .takes_value(true),
        )
        .get_matches();

    let cmd = if let Some(file) = matches.value_of("file-to-csv") {
        Cmd::FileToCsv(PathBuf::from(file))
    } else {
        Cmd::All
    };

    let settings_path = matches.value_of("settings").map(Path::new);

    Ok((cmd, Settings::new(settings_path)?))
}

/// Initialize the logger.
fn initialize_logger(file_path: &Path, settings: &Settings) -> Result<(), Error> {
    let mut loggers: Vec<Box<dyn SharedLogger>> = Vec::new();

    let log_level = if settings.general.debug {
        simplelog::LevelFilter::Debug
    } else {
        simplelog::LevelFilter::Info
    };

    let term_logger = TermLogger::new(
        log_level,
        simplelog::Config::default(),
        simplelog::TerminalMode::default(),
        simplelog::ColorChoice::Auto,
    );

    loggers.push(term_logger);

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
