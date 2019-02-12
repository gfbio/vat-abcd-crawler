use crate::abcd_fields::AbcdField;
use crate::abcd_version::AbcdVersion;
use failure::Error;
use failure::Fail;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;

pub type NumericMap = HashMap<String, f64>;
pub type TextualMap = HashMap<String, String>;

/// This parser processes ABCD XML files
#[derive(Debug)]
pub struct AbcdParser<'a> {
    abcd_fields: &'a HashMap<Vec<u8>, AbcdField>,
    abcd_version: AbcdVersion,
    xml_tag_path: Vec<u8>,
    xml_buffer: Vec<u8>,
    numeric_values: NumericMap,
    textual_values: TextualMap,
}

impl<'a> AbcdParser<'a> {
    pub fn new(abcd_fields: &'a HashMap<Vec<u8>, AbcdField>) -> Self {
        Self {
            abcd_fields,
            abcd_version: AbcdVersion::Unknown,
            xml_tag_path: Vec::new(),
            xml_buffer: Vec::new(),
            numeric_values: NumericMap::new(),
            textual_values: TextualMap::new(),
        }
    }

    pub fn parse(&mut self, xml_bytes: &[u8]) -> Result<AbcdResult, Error> {
        let mut xml_reader = Reader::from_reader(xml_bytes);
        xml_reader.trim_text(true);

        let mut dataset_data = None;
        let mut units = Vec::new();

        loop {
            match xml_reader.read_event(&mut self.xml_buffer) {
                Ok(Event::Start(ref e)) => {
                    self.xml_tag_path.push(b'/');
                    self.xml_tag_path.extend(Self::strip_tag(e.name()));

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

                            dataset_data = Some(self.finish_maps())
                        }
                        _ => {} // ignore other start tags
                    }
                }
                Ok(Event::End(ref e)) => {
                    const SEPARATOR_LENGTH: usize = 1;

                    let tag: Vec<u8> = Self::strip_tag(e.name()).map(|b| *b).collect();
                    let stripped_name_length = tag.len();

                    self.xml_tag_path.truncate(self.xml_tag_path.len() - stripped_name_length - SEPARATOR_LENGTH);

                    if self.xml_tag_path == b"/DataSets/DataSet/Units" && tag == b"Unit" {
//                        eprintln!("Unit Data:");
//                        dbg!(&numeric_values);
//                        dbg!(&textual_values);

                        units.push(self.finish_maps());
                    }
                }
                Ok(Event::Text(ref e)) => {
                    if let Some(abcd_field) = self.abcd_fields.get(&self.xml_tag_path) {
                        if abcd_field.numeric {
                            let string = String::from_utf8_lossy(e.escaped());
                            if let Ok(number) = string.parse::<f64>() {
                                self.numeric_values.insert(
                                    abcd_field.field.clone(),
                                    number,
                                );
                            }
                        } else {
                            self.textual_values.insert(
                                abcd_field.field.clone(),
                                String::from_utf8_lossy(e.escaped()).to_string(),
                            );
                        }
                    }
                }
                Ok(Event::Eof) => break, // exits the loop when reaching end of file
                Err(e) => panic!("Error at position {}: {:?}", xml_reader.buffer_position(), e),
                _ => (), // Ignore the other events
            }

            self.xml_buffer.clear();
        }

        self.clear(); // clear resources like buffers

        if let Some(dataset_data) = dataset_data {
            Ok(AbcdResult::new(dataset_data, units))
        } else {
            Err(AbcdContainsNoDatasetMetadata{}.into())
        }
    }

    fn clear(&mut self) {
        self.xml_tag_path.clear();
        self.xml_buffer.clear();
        self.numeric_values.clear();
        self.textual_values.clear();
    }

    fn finish_maps(&mut self) -> (NumericMap, TextualMap) {
        let result = (self.numeric_values.clone(), self.textual_values.clone());
        self.numeric_values.clear();
        self.textual_values.clear();
        result
    }

    fn strip_tag(tag: &[u8]) -> impl Iterator<Item=&u8> {
        tag.iter()
            .skip_while(|&&b| b != b':')
            .skip(1) // the ':' itself
    }
}

/// This struct reflects the result of a parsed xml file
pub struct AbcdResult {
    pub dataset_data: (NumericMap, TextualMap),
    pub units: Vec<(NumericMap, TextualMap)>,
}

impl AbcdResult {
    /// This constructor creates a new `AbcdResult` from dataset and unit data.
    pub fn new(dataset_data: (NumericMap, TextualMap), units: Vec<(NumericMap, TextualMap)>) -> Self {
        AbcdResult { dataset_data, units }
    }
}

/// This error occurs when a dataset's metadata is missing.
#[derive(Debug, Default, Fail)]
#[fail(display = "ABCD file contains no dataset metadata.")]
struct AbcdContainsNoDatasetMetadata {}
