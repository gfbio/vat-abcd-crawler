use mockito::{mock, Matcher};
use reqwest::Client;
use std::collections::HashMap;

#[test]
fn mockito_expect_body() {
    let _webserver = mock("POST", Matcher::Any)
        .match_body("FOOBAR")
        .with_body("GOTCHA")
        .create();

    let client = Client::new();
    let mut response = client
        .post(&mockito::server_url())
        .body("FOOBAR")
        .send()
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().unwrap(), "GOTCHA");
}

#[test]
fn mockito_expect_json() {
    const JSON_STRING: &str = r#"{"foo" : "bar"}"#;

    let _webserver = mock("POST", Matcher::Any)
        .match_body(Matcher::JsonString(JSON_STRING.into()))
        .with_body("GOTCHA")
        .create();

    let client = Client::new();
    let mut response = client
        .post(&mockito::server_url())
        .body(JSON_STRING)
        .send()
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().unwrap(), "GOTCHA");
}

#[test]
fn mockito_expect_json_from_map() {
    let _webserver = mock("POST", Matcher::Any)
        .match_body(Matcher::JsonString(r#"{"foo" : "bar"}"#.into()))
        .with_body("GOTCHA")
        .create();

    let mut map = HashMap::new();
    map.insert("foo", "bar");

    let client = Client::new();
    let mut response = client
        .post(&mockito::server_url())
        .json(&map)
        .send()
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().unwrap(), "GOTCHA");
}
