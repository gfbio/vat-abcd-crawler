use mockito::{mock, Matcher, Mock};

pub struct MockWebserver {
    _mock: Mock,
}

impl MockWebserver {
    pub fn from_text(path: &str, method: &str, text: &str) -> Self {
        Self {
            _mock: mock(method, path).with_body(text).create(),
        }
    }

    pub fn from_json(path: &str, method: &str, json_string: &str) -> Self {
        Self {
            _mock: mock(method, path)
                .with_header("content-type", "application/json")
                .with_body(json_string)
                .create(),
        }
    }

    pub fn from_json_with_json_condition(
        path: &str,
        method: &str,
        json_condition: &str,
        json_result: &str,
    ) -> Self {
        Self {
            _mock: mock(method, path)
                .match_body(Matcher::JsonString(json_condition.to_string()))
                .with_header("content-type", "application/json")
                .with_body(json_result)
                .create(),
        }
    }

    pub fn webserver_root_url(&self) -> String {
        mockito::server_url()
    }
}
