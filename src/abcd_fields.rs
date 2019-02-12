use failure::Error;
use std::path::Path;
use std::fs::File;
use std::io::BufReader;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// This struct reflect a field within the ABCD fields specification file.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AbcdField {
    pub field: String,
    pub numeric: bool,
    pub vat_mandatory: bool,
    pub gfbio_mandatory: bool,
    pub global_field: bool,
    pub unit: String,
}

/// This function loads all `AbcdField`s from a given file path.
/// It returns a map from the binary field name to the `AbcdField`.
pub fn load_abcd_fields(path: &Path) -> Result<HashMap<Vec<u8>, AbcdField>, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    Ok(fields_to_map(serde_json::from_reader(reader)?))
}

/// This function creates a map from binary field name to `AbcdField` from a list of `AbcdField`s.
fn fields_to_map(fields: Vec<AbcdField>) -> HashMap<Vec<u8>, AbcdField> {
    let mut map = HashMap::with_capacity(fields.len());
    for field in fields {
        map.insert(field.field.as_bytes().into(), field);
    }
    map
}