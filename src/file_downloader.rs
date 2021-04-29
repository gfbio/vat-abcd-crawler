use failure::Error;
use std::{fs::File, path::Path};

pub struct FileDownloader {
    url: String,
}

impl FileDownloader {
    pub fn from_url(url: &str) -> Self {
        Self { url: url.into() }
    }

    pub fn to_path(&self, path: &Path) -> Result<(), Error> {
        let mut response = reqwest::blocking::get(&self.url)?;

        if !response.status().is_success() {
            return Err(failure::err_msg(format!(
                "Webserver responded with code: {}",
                response.status(),
            )));
        }

        let mut output_file = File::create(&path)?;

        response.copy_to(&mut output_file)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{create_empty_temp_file, MockWebserver};
    use std::fs;

    #[test]
    fn download_file() {
        const CONTENT: &str = "foobar";

        let webserver = MockWebserver::from_text("/", "GET", CONTENT);
        let download_file = create_empty_temp_file();

        FileDownloader::from_url(&webserver.webserver_root_url())
            .to_path(&download_file)
            .unwrap();

        let file_content = fs::read_to_string(download_file).unwrap();

        assert_eq!(CONTENT, file_content);
    }
}
