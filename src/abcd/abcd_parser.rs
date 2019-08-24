use std::collections::HashMap;

use failure::Error;
use failure::Fail;
use quick_xml::events::Event;
use quick_xml::Reader;

use crate::abcd::{AbcdFields, AbcdVersion};
use crate::settings::AbcdSettings;
use crate::vat_type::VatType;

pub type ValueMap = HashMap<String, VatType>;

/// This parser processes ABCD XML files.
#[derive(Debug)]
pub struct AbcdParser<'a> {
    abcd_fields: &'a AbcdFields,
    abcd_settings: &'a AbcdSettings,
    abcd_version: AbcdVersion,
    xml_tag_path: Vec<u8>,
    xml_buffer: Vec<u8>,
    values: ValueMap,
}

impl<'a> AbcdParser<'a> {
    /// Create a new `AbcdParser`.
    pub fn new(abcd_settings: &'a AbcdSettings, abcd_fields: &'a AbcdFields) -> Self {
        Self {
            abcd_settings,
            abcd_fields,
            abcd_version: AbcdVersion::Unknown,
            xml_tag_path: Vec::new(),
            xml_buffer: Vec::new(),
            values: ValueMap::new(),
        }
    }

    /// Parse a binary XML file to `AbcdResult`s.
    pub fn parse(
        &mut self,
        dataset_id: &str,
        dataset_path: &str,
        landing_page_proposal: &str,
        provider_name: &str,
        xml_bytes: &[u8],
    ) -> Result<AbcdResult, Error> {
        let mut xml_reader = Reader::from_reader(xml_bytes);
        xml_reader.trim_text(true);

        let mut dataset_data = None;
        let mut units = Vec::new();

        loop {
            match xml_reader.read_event(&mut self.xml_buffer) {
                Ok(Event::Start(ref e)) => {
                    self.xml_tag_path.push(b'/');
                    self.xml_tag_path.extend(Self::strip_tag(e.name()));

                    //                    debug!("XML START: {}", String::from_utf8_lossy(&self.xml_tag_path));

                    match self.xml_tag_path.as_slice() {
                        b"/DataSets" => {
                            for attribute in e.attributes().filter_map(Result::ok) {
                                match attribute.value.as_ref() {
                                    b"http://www.tdwg.org/schemas/abcd/2.06" => {
                                        self.abcd_version = AbcdVersion::Version206;
                                        break;
                                    }
                                    b"http://www.tdwg.org/schemas/abcd/2.1" => {
                                        self.abcd_version = AbcdVersion::Version210;
                                        break;
                                    }
                                    _ => {}
                                }
                            }

                            //                            dbg!(&abcd_version);
                        }
                        b"/DataSets/DataSet/Units" => {
                            //                            eprintln!("Dataset Metadata:");
                            //                            dbg!(&numeric_values);
                            //                            dbg!(&textual_values);
                            //                            dbg!(units);

                            dataset_data = Some(self.finish_map())
                        }
                        _ => {} // ignore other start tags
                    }
                }
                Ok(Event::End(ref e)) => {
                    const SEPARATOR_LENGTH: usize = 1;

                    let tag: Vec<u8> = Self::strip_tag(e.name()).cloned().collect();
                    let stripped_name_length = tag.len();

                    self.xml_tag_path.truncate(
                        self.xml_tag_path.len() - stripped_name_length - SEPARATOR_LENGTH,
                    );

                    if self.xml_tag_path == b"/DataSets/DataSet/Units" && tag == b"Unit" {
                        //                        eprintln!("Unit Data:");
                        //                        dbg!(&numeric_values);
                        //                        dbg!(&textual_values);

                        units.push(self.finish_map());
                    }
                }
                Ok(Event::Text(ref e)) => {
                    if let Some(abcd_field) = self.abcd_fields.value_of(&self.xml_tag_path) {
                        if abcd_field.numeric {
                            let string = String::from_utf8_lossy(e.escaped());
                            if let Ok(number) = string.parse::<f64>() {
                                self.values.insert(abcd_field.name.clone(), number.into());
                            }
                        } else {
                            self.values.insert(
                                abcd_field.name.clone(),
                                String::from_utf8_lossy(e.escaped()).into(),
                            );
                        }
                    }
                }
                Ok(Event::Eof) => break, // exits the loop when reaching end of file
                Err(e) => panic!(
                    "Error at position {}: {:?}",
                    xml_reader.buffer_position(),
                    e
                ),
                _ => (), // ignore all other events
            }

            self.xml_buffer.clear();
        }

        self.clear(); // clear resources like buffers

        if let Some(dataset_data) = dataset_data {
            let landing_page = if let Some(VatType::Textual(value)) =
                dataset_data.get(&self.abcd_settings.landing_page_field)
            {
                value
            } else {
                landing_page_proposal
            };

            Ok(AbcdResult::new(
                dataset_id.into(),
                dataset_path.into(),
                landing_page.into(),
                provider_name.into(),
                dataset_data,
                units,
            ))
        } else {
            Err(AbcdContainsNoDatasetMetadata {}.into())
        }
    }

    /// Clear all buffers.
    fn clear(&mut self) {
        self.xml_tag_path.clear();
        self.xml_buffer.clear();
        self.values.clear();
    }

    /// Clear value map and return the old values.
    fn finish_map(&mut self) -> ValueMap {
        let result = self.values.clone();
        self.values.clear();
        result
    }

    /// Strip the namespace from a tag.
    fn strip_tag(tag: &[u8]) -> impl Iterator<Item = &u8> {
        let has_colon = tag.iter().any(|&b| b == b':');
        tag.iter()
            .skip_while(move |&&b| has_colon && b != b':')
            .skip(if has_colon { 1 } else { 0 }) // the ':' itself
    }
}

/// This struct reflects the result of a parsed xml file with miscellaneous additional static meta data
pub struct AbcdResult {
    pub dataset_id: String,
    pub dataset_path: String,
    pub landing_page: String,
    pub provider_name: String,
    pub dataset: ValueMap,
    pub units: Vec<ValueMap>,
}

impl AbcdResult {
    /// This constructor creates a new `AbcdResult` from dataset and unit data.
    pub fn new(
        dataset_id: String,
        dataset_path: String,
        landing_page: String,
        provider_name: String,
        dataset_data: ValueMap,
        units_data: Vec<ValueMap>,
    ) -> Self {
        AbcdResult {
            dataset_id,
            dataset_path,
            landing_page,
            provider_name,
            dataset: dataset_data,
            units: units_data,
        }
    }
}

/// This error occurs when a dataset's metadata is missing.
#[derive(Debug, Default, Fail)]
#[fail(display = "ABCD file contains no dataset metadata.")]
struct AbcdContainsNoDatasetMetadata {}

#[cfg(test)]
mod tests {
    use crate::test_utils;

    use super::*;

    const TECHNICAL_CONTACT_NAME: &str = "TECHNICAL CONTACT NAME";
    const DESCRIPTION_TITLE: &str = "DESCRIPTION TITLE";
    const LANDING_PAGE: &str = "http://LANDING-PAGE/";
    const UNIT_ID: &str = "UNIT ID";
    const UNIT_LONGITUDE: f64 = 10.911;
    const UNIT_LATITUDE: f64 = 49.911;
    const UNIT_SPATIAL_DATUM: &str = "TECHNICAL WGS84 EMAIL";

    #[test]
    fn simple_file() {
        let abcd_fields = create_abcd_fields();
        let abcd_settings = AbcdSettings {
            fields_file: "".into(),
            landing_page_field: "/DataSets/DataSet/Metadata/Description/Representation/URI".into(),
            storage_dir: "raw_data".into(),
        };

        let test_file = create_file_as_bytes();

        let mut parser = AbcdParser::new(&abcd_settings, &abcd_fields);

        let dataset_id = "dataset_id";
        let dataset_path = "dataset_path";
        let landing_page_proposal = "landing_page proposal";
        let provider_name = "provider_id";

        let result = parser
            .parse(
                dataset_id,
                dataset_path,
                landing_page_proposal,
                provider_name,
                &test_file,
            )
            .expect("Unable to parse bytes");

        assert_eq!(result.dataset_id, dataset_id);
        assert_eq!(result.dataset_path, dataset_path);
        assert_eq!(result.landing_page, LANDING_PAGE);
        assert_eq!(result.provider_name, provider_name);

        assert_eq!(
            Some(&VatType::Textual(TECHNICAL_CONTACT_NAME.into())),
            result
                .dataset
                .get("/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name")
        );
        assert_eq!(
            Some(&VatType::Textual(DESCRIPTION_TITLE.into())),
            result
                .dataset
                .get("/DataSets/DataSet/Metadata/Description/Representation/Title")
        );

        assert_eq!(result.units.len(), 1);

        let unit = result.units.get(0).unwrap();

        assert_eq!(
            Some(&VatType::Textual(UNIT_ID.into())),
            unit.get("/DataSets/DataSet/Units/Unit/UnitID")
        );
        assert_eq!(
            Some(&VatType::Textual(UNIT_SPATIAL_DATUM.into())),
            unit.get("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum")
        );

        if let (Some(&VatType::Numeric(longitude)), Some(&VatType::Numeric(latitude))) = (
            unit.get("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal"),
            unit.get("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal")
        ) {
            assert!(f64::abs(longitude - UNIT_LONGITUDE) < 0.01);
            assert!(f64::abs(latitude - UNIT_LATITUDE) < 0.01);
        }
    }

    fn create_file_as_bytes() -> Vec<u8> {
        format!(
            r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <abcd:DataSets xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xmlns:abcd="http://www.tdwg.org/schemas/abcd/2.06"
                           xsi:schemaLocation=" http://www.tdwg.org/schemas/abcd/2.06 http://rs.tdwg.org/abcd/2.06/ABCD_2.06.xsd">
            <abcd:DataSet>
                <abcd:TechnicalContacts>
                    <abcd:TechnicalContact>
                        <abcd:Name>{TECHNICAL_CONTACT_NAME}</abcd:Name>
                    </abcd:TechnicalContact>
                </abcd:TechnicalContacts>
                <abcd:Metadata>
                    <abcd:Description>
                        <abcd:Representation language="en">
                            <abcd:Title>{DESCRIPTION_TITLE}</abcd:Title>
                            <abcd:URI>{LANDING_PAGE}</abcd:URI>
                        </abcd:Representation>
                    </abcd:Description>
                </abcd:Metadata>
                <abcd:Units>
                    <abcd:Unit>
                        <abcd:UnitID>{UNIT_ID}</abcd:UnitID>
                        <abcd:Gathering>
                            <abcd:SiteCoordinateSets>
                                <abcd:SiteCoordinates>
                                    <abcd:CoordinatesLatLong>
                                        <abcd:LongitudeDecimal>{UNIT_LONGITUDE}</abcd:LongitudeDecimal>
                                        <abcd:LatitudeDecimal>{UNIT_LATITUDE}</abcd:LatitudeDecimal>
                                        <abcd:SpatialDatum>{UNIT_SPATIAL_DATUM}</abcd:SpatialDatum>
                                    </abcd:CoordinatesLatLong>
                                </abcd:SiteCoordinates>
                            </abcd:SiteCoordinateSets>
                        </abcd:Gathering>
                    </abcd:Unit>
                </abcd:Units>
            </abcd:DataSet>
            </abcd:DataSets>
            "#,
            TECHNICAL_CONTACT_NAME = TECHNICAL_CONTACT_NAME,
            DESCRIPTION_TITLE = DESCRIPTION_TITLE,
            LANDING_PAGE = LANDING_PAGE,
            UNIT_ID = UNIT_ID,
            UNIT_LONGITUDE = UNIT_LONGITUDE,
            UNIT_LATITUDE = UNIT_LATITUDE,
            UNIT_SPATIAL_DATUM = UNIT_SPATIAL_DATUM,
        ).into_bytes()
    }

    fn create_abcd_fields() -> AbcdFields {
        let fields_file = test_utils::create_temp_file(
            r#"[
                {
                    "name": "/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name",
                    "numeric": false,
                    "vatMandatory": false,
                    "gfbioMandatory": true,
                    "globalField": true,
                    "unit": ""
                },
                {
                    "name": "/DataSets/DataSet/Metadata/Description/Representation/Title",
                    "numeric": false,
                    "vatMandatory": false,
                    "gfbioMandatory": true,
                    "globalField": true,
                    "unit": ""
                },
                {
                    "name": "/DataSets/DataSet/Metadata/Description/Representation/URI",
                    "numeric": false,
                    "vatMandatory": false,
                    "gfbioMandatory": true,
                    "globalField": true,
                    "unit": ""
                },
                {
                    "name": "/DataSets/DataSet/Units/Unit/UnitID",
                    "numeric": false,
                    "vatMandatory": false,
                    "gfbioMandatory": true,
                    "globalField": false,
                    "unit": ""
                },
                {
                    "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal",
                    "numeric": true,
                    "vatMandatory": true,
                    "gfbioMandatory": true,
                    "globalField": false,
                    "unit": "°"
                },
                {
                    "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal",
                    "numeric": true,
                    "vatMandatory": true,
                    "gfbioMandatory": true,
                    "globalField": false,
                    "unit": "°"
                },
                {
                    "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum",
                    "numeric": false,
                    "vatMandatory": false,
                    "gfbioMandatory": true,
                    "globalField": false,
                    "unit": ""
                }
            ]"#,
        );

        AbcdFields::from_path(&fields_file).expect("Unable to create ABCD Fields Spec")
    }
}
