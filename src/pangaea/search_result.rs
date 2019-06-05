use failure::Error;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SearchResult {
    #[serde(rename = "_scroll_id")]
    scroll_id: String,
    hits: SearchResultHits,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct SearchResultHits {
    total: u64,
    hits: Vec<SearchResultEntry>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct SearchResultEntry {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_source")]
    source: SearchResultEntrySource,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct SearchResultEntrySource {
    citation_publisher: String,
    datalink: String,
}

impl SearchResult {
    fn from_url(url: &str) -> Result<Self, Error> {
        const SCROLL_TIMEOUT: &str = "1m";
        reqwest::Client::new()
            .get(&format!(
                "{url}?scroll={scroll}",
                url = url,
                scroll = SCROLL_TIMEOUT,
            ))
            .json(
                r#"{
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
                }"#,
            )
            .send()?
            .json::<Self>()
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils;

    const CITATION_PUBLISHER: &str = "Test Publisher";
    const CITATION_PUBLISHER_2: &str = "Test Publisher";
    const DATALINK: &str = "https://foobar.de";
    const DATALINK_2: &str = "https://foobar2.de";
    const RESULT_ID: &str = "test_id";
    const RESULT_ID_2: &str = "test_id";
    const SEARCH_RESULT_HITS: u64 = 64;
    const SCROLL_ID: &str = "SCROLL_ID_SCROLL_ID";

    const SEARCH_RESULT_ENTRY_SOURCE_JSON: fn() -> String = || {
        format!(
            r#"
            {{
                "citation_publisher": "{citation_publisher}",
                "datalink": "{datalink}"
            }}
        "#,
            citation_publisher = CITATION_PUBLISHER,
            datalink = DATALINK,
        )
    };
    const SEARCH_RESULT_ENTRY_SOURCE_JSON_2: fn() -> String = || {
        format!(
            r#"
            {{
                "citation_publisher": "{citation_publisher}",
                "datalink": "{datalink}"
            }}
        "#,
            citation_publisher = CITATION_PUBLISHER_2,
            datalink = DATALINK_2,
        )
    };
    const SEARCH_RESULT_ENTRY_JSON: fn() -> String = || {
        format!(
            r#"
                {{
                    "_id": "{test_id}",
                    "_source": {source}
                }}
            "#,
            test_id = RESULT_ID,
            source = SEARCH_RESULT_ENTRY_SOURCE_JSON(),
        )
    };
    const SEARCH_RESULT_ENTRY_JSON_2: fn() -> String = || {
        format!(
            r#"
                {{
                    "_id": "{test_id}",
                    "_source": {source}
                }}
            "#,
            test_id = RESULT_ID_2,
            source = SEARCH_RESULT_ENTRY_SOURCE_JSON_2(),
        )
    };
    const SEARCH_RESULT_HITS_JSON: fn() -> String = || {
        format!(
            r#"
            {{
                "total": {hits},
                "max_score": 1.0,
                "hits": [
                    {r1},
                    {r2}
                ]
            }}
            "#,
            hits = SEARCH_RESULT_HITS,
            r1 = SEARCH_RESULT_ENTRY_JSON(),
            r2 = SEARCH_RESULT_ENTRY_JSON_2(),
        )
    };
    const SEARCH_RESULT_JSON: fn() -> String = || {
        format!(
            r#"
            {{
                "_scroll_id": "{scroll_id}",
                "took": 1373,
                "hits": {hits}
            }}
            "#,
            scroll_id = SCROLL_ID,
            hits = SEARCH_RESULT_HITS_JSON(),
        )
    };

    #[test]
    fn parse_search_result_entry_source() {
        let search_result_entry_source =
            serde_json::from_str::<SearchResultEntrySource>(&SEARCH_RESULT_ENTRY_SOURCE_JSON())
                .unwrap();

        assert_eq!(
            search_result_entry_source,
            SearchResultEntrySource {
                citation_publisher: CITATION_PUBLISHER.into(),
                datalink: DATALINK.into(),
            }
        )
    }

    #[test]
    fn parse_search_result_entry() {
        let search_result_entry =
            serde_json::from_str::<SearchResultEntry>(&SEARCH_RESULT_ENTRY_JSON()).unwrap();

        assert_eq!(
            search_result_entry,
            SearchResultEntry {
                id: RESULT_ID.to_string(),
                source: SearchResultEntrySource {
                    citation_publisher: CITATION_PUBLISHER.into(),
                    datalink: DATALINK.into(),
                },
            }
        )
    }

    #[test]
    fn parse_search_result_hits() {
        let search_result_hits =
            serde_json::from_str::<SearchResultHits>(&SEARCH_RESULT_HITS_JSON()).unwrap();

        assert_eq!(
            search_result_hits,
            SearchResultHits {
                total: SEARCH_RESULT_HITS,
                hits: vec![
                    SearchResultEntry {
                        id: RESULT_ID.to_string(),
                        source: SearchResultEntrySource {
                            citation_publisher: CITATION_PUBLISHER.into(),
                            datalink: DATALINK.into(),
                        },
                    },
                    SearchResultEntry {
                        id: RESULT_ID_2.to_string(),
                        source: SearchResultEntrySource {
                            citation_publisher: CITATION_PUBLISHER_2.into(),
                            datalink: DATALINK_2.into(),
                        },
                    },
                ],
            }
        )
    }

    #[test]
    fn parse_search_result() {
        let search_result = serde_json::from_str::<SearchResult>(&SEARCH_RESULT_JSON()).unwrap();

        assert_eq!(search_result.scroll_id, SCROLL_ID);
        assert_eq!(search_result.hits.hits.len(), 2);
    }

    #[test]
    fn parse_webserver_result() {
        let _webserver = test_utils::create_json_webserver(&SEARCH_RESULT_JSON());

        let search_result = SearchResult::from_url(&test_utils::webserver_url()).unwrap();

        assert_eq!(search_result.scroll_id, SCROLL_ID);
        assert_eq!(search_result.hits.hits.len(), 2);
    }
}
