fn main() {
    let matches = App::new("VAT ABCD Crawler")
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .arg(Arg::with_name("input")
            .short("i")
            .long("input")
            .value_name("FILE")
            .help("Specify the input file")
            .required(true)
            .takes_value(true))
        .get_matches();

    let input = Path::new(
        matches.value_of("input").expect("There must be an input present.")
    );
    let file = File::open(input).expect(&format!("Unable to open file {}.", input.display()));
    let archive_reader = BufReader::new(file);

    dbg!(&archive_reader);

    let mut archive = ZipArchive::new(archive_reader).expect("Corrupt zip archive.");

    let mut abcd_version = AbcdVersion::Unknown;

    for i in 0..archive.len() {
        let archive_file = archive.by_index(i).expect("Unable to access {}th file of archive.");
        dbg!(archive_file.sanitized_name());

        let archive_file_reader = BufReader::new(archive_file);

        let mut xml_reader = Reader::from_reader(archive_file_reader);
        xml_reader.trim_text(true);

        let mut xml_buffer = Vec::new();

        loop {
            match xml_reader.read_event(&mut xml_buffer) {
                Ok(Event::Start(ref e)) => {
                    xml_tag_path.push(b'/');
                    xml_tag_path.extend(strip_tag(e.name()));

                    match xml_tag_path.as_slice() {
                        b"/DataSets" => {
                            for attribute in e.attributes().filter_map(Result::ok) {
                                match attribute.value.as_ref() {
                                    b"http://www.tdwg.org/schemas/abcd/2.06" => {
                                        abcd_version = AbcdVersion::Version206;
                                        break;
                                    }
                                    b"http://www.tdwg.org/schemas/abcd/2.1" => {
                                        abcd_version = AbcdVersion::Version210;
                                        break;
                                    }
                                    _ => {}
                                }
                            }

                            dbg!(&abcd_version);
                        }
                        b"/DataSets/DataSet/Units" => {
                            eprintln!("Dataset Metadata:");
                            dbg!(&numeric_values);
                            dbg!(&textual_values);
                            dbg!(units);

                            numeric_values.clear();
                            textual_values.clear();
                        }
                        tag_ => {
//                            let string = String::from_utf8_lossy(tag);
//                            dbg!(string);
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    const SEPARATOR_LENGTH: usize = 1;

                    let tag: Vec<u8> = strip_tag(e.name()).map(|b| *b).collect();
//                    let stripped_name_length = strip_tag(e.name()).count();
                    let stripped_name_length = tag.len();

                    xml_tag_path.truncate(xml_tag_path.len() - stripped_name_length - SEPARATOR_LENGTH);

                    if xml_tag_path == b"/DataSets/DataSet/Units" && tag == b"Unit" {
//                        eprintln!("Unit Data:");
//                        dbg!(&numeric_values);
//                        dbg!(&textual_values);
                        units += 1;

                        numeric_values.clear();
                        textual_values.clear();
                    }
                }
                Ok(Event::Text(ref e)) => {
                    if let Some(abcd_field) = abcd_fields.get(&xml_tag_path) {
                        if abcd_field.numeric {
                            let string = String::from_utf8_lossy(e.escaped());
                            if let Ok(number) = string.parse::<f64>() {
                                numeric_values.insert(
                                    abcd_field.field.clone(),
                                    number,
                                );
                            }
                        } else {
                            textual_values.insert(
                                abcd_field.field.clone(),
                                String::from_utf8_lossy(e.escaped()).to_string(),
                            );
                        }
                    }
                }
                Ok(Event::Eof) => break, // exits the loop when reaching end of file
                Err(e) => panic!("Error at position {}: {:?}", xml_reader.buffer_position(), e),
                _ => (), // There are several other `Event`s we do not consider here
            }

            xml_buffer.clear();
        }
    }
}