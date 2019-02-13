use crate::abcd_fields::AbcdField;
use crate::abcd_version::AbcdVersion;
use crate::vat_type::VatType;
use failure::Error;
use failure::Fail;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;

pub type ValueMap = HashMap<String, VatType>;

/// This parser processes ABCD XML files.
#[derive(Debug)]
pub struct AbcdParser<'a> {
    abcd_fields: &'a HashMap<Vec<u8>, AbcdField>,
    abcd_version: AbcdVersion,
    xml_tag_path: Vec<u8>,
    xml_buffer: Vec<u8>,
    values: ValueMap,
}

impl<'a> AbcdParser<'a> {
    /// Create a new `AbcdParser`.
    pub fn new(abcd_fields: &'a HashMap<Vec<u8>, AbcdField>) -> Self {
        Self {
            abcd_fields,
            abcd_version: AbcdVersion::Unknown,
            xml_tag_path: Vec::new(),
            xml_buffer: Vec::new(),
            values: ValueMap::new(),
        }
    }

    /// Parse a binary XML file to `AbcdResult`s.
    pub fn parse(&mut self, dataset_path: &str, xml_bytes: &[u8]) -> Result<AbcdResult, Error> {
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

                    self.xml_tag_path.truncate(self.xml_tag_path.len() - stripped_name_length - SEPARATOR_LENGTH);

                    if self.xml_tag_path == b"/DataSets/DataSet/Units" && tag == b"Unit" {
//                        eprintln!("Unit Data:");
//                        dbg!(&numeric_values);
//                        dbg!(&textual_values);

                        units.push(self.finish_map());
                    }
                }
                Ok(Event::Text(ref e)) => {
                    if let Some(abcd_field) = self.abcd_fields.get(&self.xml_tag_path) {
                        if abcd_field.numeric {
                            let string = String::from_utf8_lossy(e.escaped());
                            if let Ok(number) = string.parse::<f64>() {
                                self.values.insert(
                                    abcd_field.field.clone(),
                                    number.into(),
                                );
                            }
                        } else {
                            self.values.insert(
                                abcd_field.field.clone(),
                                String::from_utf8_lossy(e.escaped()).into(),
                            );
                        }
                    }
                }
                Ok(Event::Eof) => break, // exits the loop when reaching end of file
                Err(e) => panic!("Error at position {}: {:?}", xml_reader.buffer_position(), e),
                _ => (), // ignore all other events
            }

            self.xml_buffer.clear();
        }

        self.clear(); // clear resources like buffers

        if let Some(dataset_data) = dataset_data {
            Ok(AbcdResult::new(dataset_path.into(), dataset_data, units))
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
    fn strip_tag(tag: &[u8]) -> impl Iterator<Item=&u8> {
        let has_colon = tag.iter().any(|&b| b == b':');
        tag.iter()
            .skip_while(move |&&b| has_colon && b != b':')
            .skip(if has_colon { 1 } else { 0 }) // the ':' itself
    }
}

/// This struct reflects the result of a parsed xml file
pub struct AbcdResult {
    pub dataset_path: String,
    pub dataset: ValueMap,
    pub units: Vec<ValueMap>,
}

impl AbcdResult {
    /// This constructor creates a new `AbcdResult` from dataset and unit data.
    pub fn new(dataset_path: String, dataset_data: ValueMap, units_data: Vec<ValueMap>) -> Self {
        AbcdResult { dataset_path, dataset: dataset_data, units: units_data }
    }
}

/// This error occurs when a dataset's metadata is missing.
#[derive(Debug, Default, Fail)]
#[fail(display = "ABCD file contains no dataset metadata.")]
struct AbcdContainsNoDatasetMetadata {}
