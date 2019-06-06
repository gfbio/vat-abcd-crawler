use failure::Error;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

pub struct FileDownloader {
    url: String,
}

impl FileDownloader {
    pub fn from_url(url: &str) -> Self {
        Self { url: url.into() }
    }

    pub fn to_path(&self, path: &Path) -> Result<(), Error> {
        let mut response = reqwest::get(&self.url)?;

        let output_file = File::create(&path)?;

        let mut writer = BufWriter::new(&output_file);
        std::io::copy(&mut response, &mut writer)?;

        Ok(())
    }
}
