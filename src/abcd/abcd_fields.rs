use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use failure::Error;
use serde::{Deserialize, Serialize};

/// This struct reflect a field within the ABCD fields specification file.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AbcdField {
    pub name: String,
    pub numeric: bool,
    pub vat_mandatory: bool,
    pub gfbio_mandatory: bool,
    pub global_field: bool,
    pub unit: String,
}

type BinaryString = Vec<u8>;

#[derive(Debug)]
pub struct AbcdFields {
    fields: HashMap<BinaryString, AbcdField>,
}

impl AbcdFields {
    pub fn from_path(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        Ok(Self {
            fields: Self::fields_to_map(serde_json::from_reader(reader)?),
        })
    }

    /// This function creates a map from binary field name to `AbcdField` from a list of `AbcdField`s.
    fn fields_to_map(fields: Vec<AbcdField>) -> HashMap<Vec<u8>, AbcdField> {
        let mut map = HashMap::with_capacity(fields.len());
        for field in fields {
            map.insert(field.name.as_bytes().into(), field);
        }
        map
    }

    pub fn value_of(&self, field: &[u8]) -> Option<&AbcdField> {
        self.fields.get(field)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }
}

impl<'a> IntoIterator for &'a AbcdFields {
    type Item = &'a AbcdField;
    type IntoIter = Values<'a, BinaryString, AbcdField>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.values()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempPath;

    use crate::test_utils;

    use super::*;

    #[test]
    fn simple_file() {
        let path = create_test_file_path();

        let abcd_fields = AbcdFields::from_path(&path).expect("Unable to deserialize input.");

        assert_eq!(abcd_fields.len(), 2);

        let field1 = abcd_fields
            .value_of(&b"/DataSets/DataSet/DatasetGUID".to_vec())
            .expect("Field not found");
        assert_eq!(field1.name, "/DataSets/DataSet/DatasetGUID");
        assert!(!field1.numeric);
        assert!(!field1.vat_mandatory);
        assert!(!field1.gfbio_mandatory);
        assert!(field1.global_field);
        assert!(field1.unit.is_empty());

        let field2 = abcd_fields
            .value_of(&b"/DataSets/DataSet/Units/Unit/SourceInstitutionID".to_vec())
            .expect("Field not found");
        assert_eq!(
            field2.name,
            "/DataSets/DataSet/Units/Unit/SourceInstitutionID"
        );
        assert!(!field2.numeric);
        assert!(field2.vat_mandatory);
        assert!(field2.gfbio_mandatory);
        assert!(!field2.global_field);
        assert_eq!(field2.unit, "TEST");
    }

    #[test]
    fn iterate_values() {
        let path = create_test_file_path();

        let abcd_fields = AbcdFields::from_path(&path).expect("Unable to deserialize input.");

        let mut number_of_fields = 0;
        for _field in &abcd_fields {
            number_of_fields += 1;
        }

        assert_eq!(number_of_fields, 2);
    }

    fn create_test_file_path() -> TempPath {
        test_utils::create_temp_file(
            r#"[
                {
                    "name": "/DataSets/DataSet/DatasetGUID",
                    "numeric": false,
                    "vatMandatory": false,
                    "gfbioMandatory": false,
                    "globalField": true,
                    "unit": ""
                },
                {
                    "name": "/DataSets/DataSet/Units/Unit/SourceInstitutionID",
                    "numeric": false,
                    "vatMandatory": true,
                    "gfbioMandatory": true,
                    "globalField": false,
                    "unit": "TEST"
                }
            ]"#,
        )
    }
}
