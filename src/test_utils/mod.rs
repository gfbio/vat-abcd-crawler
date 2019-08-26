mod webserver;

use std::io::Write;

use tempfile::TempPath;

pub use self::webserver::MockWebserver;

pub fn create_temp_file(content: &str) -> TempPath {
    create_temp_file_with_suffix("", content)
}

pub fn create_temp_file_with_suffix(suffix: &str, content: &str) -> TempPath {
    let mut file = tempfile::Builder::new()
        .suffix(suffix)
        .tempfile()
        .expect("Unable to create test file.");

    write!(file, "{}", content).expect("Unable to write content to test file.");

    file.into_temp_path()
}

pub fn create_empty_temp_file() -> TempPath {
    tempfile::Builder::new()
        .tempfile()
        .expect("Unable to create test file.")
        .into_temp_path()
}
