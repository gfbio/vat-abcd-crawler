use crate::settings::PangaeaSettings;
use failure::Error;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct PangaeaSearchResult {
    #[serde(rename = "_scroll_id")]
    scroll_id: String,
    hits: PangaeaSearchResultHits,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct PangaeaSearchResultHits {
    total: u64,
    hits: Vec<PangaeaSearchResultEntry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct PangaeaSearchResultEntry {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_source")]
    source: PangaeaSearchResultEntrySource,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct PangaeaSearchResultEntrySource {
    citation_publisher: String,
    datalink: String,
}

impl PangaeaSearchResult {
    const SCROLL_TIMEOUT: &'static str = "1m";

    fn from_url(url: &str) -> Result<Self, Error> {
        let body = json!({
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "internal-source": "gfbio-abcd-collections"
                            }
                        },
                        {
                            "match_phrase": {
                                "type": "ABCD_Dataset"
                            }
                        },
                        {
                            "term": {
                                "accessRestricted": false
                            }
                        }
                    ]
                }
            }
        });

        reqwest::Client::new()
            .post(&format!(
                "{url}?scroll={scroll_timeout}",
                url = url,
                scroll_timeout = Self::SCROLL_TIMEOUT,
            ))
            .json(&body)
            .send()?
            .json::<Self>()
            .map_err(|e| e.into())
    }

    fn from_scroll_url(url: &str, scroll_id: &str) -> Result<Self, Error> {
        let mut body = HashMap::new();
        body.insert("scroll", Self::SCROLL_TIMEOUT);
        body.insert("scroll_id", scroll_id);

        reqwest::Client::new()
            .post(url)
            .json(&body)
            .send()?
            .json::<Self>()
            .map_err(|e| e.into())
    }

    pub fn retrieve_all_entries(
        pangaea_settings: &PangaeaSettings,
    ) -> Result<Vec<PangaeaSearchResultEntry>, Error> {
        let mut entries = Vec::new();

        let mut result = Self::from_url(&pangaea_settings.search_url)?;

        while result.hits.total > 0 {
            entries.append(&mut result.hits.hits);

            result = Self::from_scroll_url(&pangaea_settings.scroll_url, &result.scroll_id)?;
        }

        entries.append(&mut result.hits.hits);

        Ok(entries)
    }
}

impl PangaeaSearchResultEntry {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn publisher(&self) -> &str {
        &self.source.citation_publisher
    }

    pub fn download_url(&self) -> &str {
        &self.source.datalink
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::MockWebserver;
    use serde_json::Value as JsonValue;

    const CITATION_PUBLISHER: &str = "Test Publisher";
    const CITATION_PUBLISHER_2: &str = "Test Publisher 2";
    const DATALINK: &str = "https://foobar.de";
    const DATALINK_2: &str = "https://foobar2.de";
    const RESULT_ID: &str = "test_id";
    const RESULT_ID_2: &str = "test_id_2";
    const SEARCH_RESULT_HITS: u64 = 64;
    const SCROLL_ID: &str = "SCROLL_ID_SCROLL_ID";
    const SCROLL_ID_2: &str = "SCROLL_ID_SCROLL_ID_2";

    const SEARCH_RESULT_ENTRY_SOURCE_JSON: fn() -> JsonValue = || {
        json!({
            "citation_publisher": CITATION_PUBLISHER,
            "datalink": DATALINK,
        })
    };
    const SEARCH_RESULT_ENTRY_SOURCE_JSON_2: fn() -> JsonValue = || {
        json!({
            "citation_publisher": CITATION_PUBLISHER_2,
            "datalink": DATALINK_2,
        })
    };
    const SEARCH_RESULT_ENTRY_JSON: fn() -> JsonValue = || {
        json!({
            "_id": RESULT_ID,
            "_source": SEARCH_RESULT_ENTRY_SOURCE_JSON(),
        })
    };
    const SEARCH_RESULT_ENTRY_JSON_2: fn() -> JsonValue = || {
        json!({
            "_id": RESULT_ID_2,
            "_source": SEARCH_RESULT_ENTRY_SOURCE_JSON_2(),
        })
    };
    const SEARCH_RESULT_HITS_JSON: fn() -> JsonValue = || {
        json!({
            "total": SEARCH_RESULT_HITS,
            "max_score": 1.0,
            "hits": [
                SEARCH_RESULT_ENTRY_JSON(),
                SEARCH_RESULT_ENTRY_JSON_2(),
            ],
        })
    };
    const SEARCH_RESULT_JSON: fn() -> JsonValue = || {
        json!({
            "_scroll_id": SCROLL_ID,
            "took": 1373,
            "hits": SEARCH_RESULT_HITS_JSON(),
        })
    };

    #[test]
    fn parse_search_result_entry_source() {
        let search_result_entry_source = serde_json::from_str::<PangaeaSearchResultEntrySource>(
            &SEARCH_RESULT_ENTRY_SOURCE_JSON().to_string(),
        )
        .unwrap();

        assert_eq!(
            search_result_entry_source,
            PangaeaSearchResultEntrySource {
                citation_publisher: CITATION_PUBLISHER.into(),
                datalink: DATALINK.into(),
            }
        )
    }

    #[test]
    fn parse_search_result_entry() {
        let search_result_entry = serde_json::from_str::<PangaeaSearchResultEntry>(
            &SEARCH_RESULT_ENTRY_JSON().to_string(),
        )
        .unwrap();

        assert_eq!(
            search_result_entry,
            PangaeaSearchResultEntry {
                id: RESULT_ID.to_string(),
                source: PangaeaSearchResultEntrySource {
                    citation_publisher: CITATION_PUBLISHER.into(),
                    datalink: DATALINK.into(),
                },
            }
        )
    }

    #[test]
    fn parse_search_result_hits() {
        let search_result_hits =
            serde_json::from_str::<PangaeaSearchResultHits>(&SEARCH_RESULT_HITS_JSON().to_string())
                .unwrap();

        assert_eq!(
            search_result_hits,
            PangaeaSearchResultHits {
                total: SEARCH_RESULT_HITS,
                hits: vec![
                    PangaeaSearchResultEntry {
                        id: RESULT_ID.to_string(),
                        source: PangaeaSearchResultEntrySource {
                            citation_publisher: CITATION_PUBLISHER.into(),
                            datalink: DATALINK.into(),
                        },
                    },
                    PangaeaSearchResultEntry {
                        id: RESULT_ID_2.to_string(),
                        source: PangaeaSearchResultEntrySource {
                            citation_publisher: CITATION_PUBLISHER_2.into(),
                            datalink: DATALINK_2.into(),
                        },
                    },
                ],
            }
        );
    }

    #[test]
    fn parse_search_result() {
        let search_result =
            serde_json::from_str::<PangaeaSearchResult>(&SEARCH_RESULT_JSON().to_string()).unwrap();

        assert_eq!(search_result.scroll_id, SCROLL_ID);
        assert_eq!(search_result.hits.hits.len(), 2);
    }

    #[test]
    fn parse_webserver_result() {
        let webserver = MockWebserver::from_json(
            &format!("/?scroll={}", PangaeaSearchResult::SCROLL_TIMEOUT),
            "POST",
            &SEARCH_RESULT_JSON().to_string(),
        );

        let search_result = PangaeaSearchResult::from_url(&webserver.webserver_root_url()).unwrap();

        assert_eq!(search_result.scroll_id, SCROLL_ID);
        assert_eq!(search_result.hits.hits.len(), 2);
    }

    #[test]
    fn parse_scroll_result() {
        let webserver = MockWebserver::from_json("/", "POST", &SEARCH_RESULT_JSON().to_string());

        let search_result =
            PangaeaSearchResult::from_scroll_url(&webserver.webserver_root_url(), SCROLL_ID)
                .unwrap();

        assert_eq!(search_result.scroll_id, SCROLL_ID);
        assert_eq!(search_result.hits.hits.len(), 2);
    }

    #[test]
    fn collect_multiple_request_data() {
        let _m1 =
            MockWebserver::from_json("/?scroll=1m", "POST", &SEARCH_RESULT_JSON().to_string());
        let _m2 = MockWebserver::from_json_with_json_condition(
            "/scroll",
            "POST",
            &json!({
              "scroll" : PangaeaSearchResult::SCROLL_TIMEOUT,
              "scroll_id" : SCROLL_ID,
            })
            .to_string(),
            &json!({
                "_scroll_id": SCROLL_ID_2,
                "took": 1373,
                "hits": {
                    "total": SEARCH_RESULT_HITS, // <-- CONTINUE
                    "hits": [
                        SEARCH_RESULT_ENTRY_JSON(),
                        SEARCH_RESULT_ENTRY_JSON_2(),
                    ],
                },
            })
            .to_string(),
        );
        let _m3 = MockWebserver::from_json_with_json_condition(
            "/scroll",
            "POST",
            &json!({
              "scroll" : PangaeaSearchResult::SCROLL_TIMEOUT,
              "scroll_id" : SCROLL_ID_2,
            })
            .to_string(),
            &json!({
                "_scroll_id": SCROLL_ID_2,
                "took": 1373,
                "hits": {
                    "total": 0, // <-- NO CONTINUE
                    "hits": [
                        SEARCH_RESULT_ENTRY_JSON(),
                    ],
                },
            })
            .to_string(),
        );

        assert_eq!(_m2.webserver_root_url(), _m3.webserver_root_url());

        let entries = PangaeaSearchResult::retrieve_all_entries(&PangaeaSettings {
            search_url: _m1.webserver_root_url(),
            scroll_url: format!("{}/scroll", _m2.webserver_root_url()),
        })
        .unwrap();

        assert_eq!(5, entries.len());

        let entry = &entries[0];
        assert_eq!(RESULT_ID, entry.id());
        assert_eq!(DATALINK, entry.download_url());
        assert_eq!(CITATION_PUBLISHER, entry.publisher());
    }
}
