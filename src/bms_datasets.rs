use failure::Error;
use failure::Fail;
use std::path::Path;
use std::fs::File;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::io::BufWriter;

/// This struct contains dataset information from the BMS
#[derive(Debug, Deserialize, Serialize)]
pub struct BmsDataset {
    pub provider_datacenter: String,
    pub provider_url: String,
    pub dsa: String,
    pub dataset: String,
    pub xml_archives: Vec<BmsXmlArchive>,
}

/// This struct contains archive download information for a BMS dataset.
#[derive(Debug, Deserialize, Serialize)]
pub struct BmsXmlArchive {
    pub id: String,
    pub xml_archive: String,
    pub latest: bool,
}

impl BmsDataset {
    pub fn get_latest_archive(&self) -> Result<&BmsXmlArchive, DatasetContainsNoFileError> {
        self.xml_archives.iter()
            .find(|archive| archive.latest) // get latest archive version
            .ok_or_else(|| DatasetContainsNoFileError::new(&self.dataset))
    }
}

/// This function downloads a list of dataset information from the BMS.
pub fn load_bms_datasets(url: &str) -> Result<Vec<BmsDataset>, Error> {
    Ok(
        reqwest::Client::new()
            .get(url)
            .send()?
            .json()?
    )
}

/// This struct combines dataset information and a path to the downloaded archive file.
#[derive(Debug)]
pub struct DownloadedBmsDataset<'d> {
    pub dataset: &'d BmsDataset,
    pub path: PathBuf,
    pub url: String,
}

impl<'d> DownloadedBmsDataset<'d> {
    /// Create a new descriptor for a downloaded BMS dataset.
    pub fn new(dataset: &'d BmsDataset, path: PathBuf, url: String) -> Self {
        Self { dataset, path, url }
    }
}

/// Download all datasets into a given temporary directory.
/// This function returns an iterator over `DownloadedBmsDataset`.
pub fn download_datasets<'d, 't>(temp_dir: &'t Path, datasets: &'d [BmsDataset]) -> impl Iterator<Item=Result<DownloadedBmsDataset<'d>, Error>> + 'd {
    let temp_dir = temp_dir.to_path_buf();
    datasets.iter().enumerate().map(move |(i, dataset)| {
        let url = dataset.get_latest_archive()?.xml_archive.clone();
        let download_file_path = temp_dir.join(Path::new(&format!("{}.zip", i)));
        download_dataset(url, download_file_path, dataset)
    })
}

/// This error occurs when it is not possible to download a dataset archive.
#[derive(Debug, Fail)]
#[fail(display = "Dataset {} contains no file to download.", dataset)]
pub struct DatasetContainsNoFileError {
    dataset: String,
}

impl DatasetContainsNoFileError {
    /// Create a new `DatasetContainsNoFileError` from a dataset name.
    pub fn new(dataset: &str) -> Self {
        Self {
            dataset: dataset.to_string(),
        }
    }
}

/// Download a dataset (the latest) into the given file path.
pub fn download_dataset(url: String, download_file_path: PathBuf, dataset: &BmsDataset) -> Result<DownloadedBmsDataset, Error> {
    let mut response = reqwest::get(&url)?;

    let output = File::create(&download_file_path)?;

    // copy file to temp path
    let mut writer = BufWriter::new(&output);
    std::io::copy(&mut response, &mut writer)?;

    Ok(DownloadedBmsDataset::new(dataset, download_file_path, url))
}
