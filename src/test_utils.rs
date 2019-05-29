use std::io::Write;

use mockito::{mock, Mock};
use tempfile::TempPath;

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

pub fn create_json_webserver(json_string: &str) -> Mock {
    mock("GET", "/")
        .with_header("content-type", "application/json")
        .with_body(json_string)
        .create()
}

pub fn webserver_url() -> String {
    mockito::server_url()
}
