use failure::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use zip::ZipArchive;

/// This struct provides a reader that processes a stream of XML files in a ZIP archive.
pub struct ArchiveReader {
    archive: ZipArchive<BufReader<File>>,
}

impl ArchiveReader {
    /// Create an `ArchiveReader` from a path to a ZIP archive.
    pub fn from_path(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let archive = ZipArchive::new(reader)?;

        Ok(Self { archive })
    }

    /// Creates an iterator that traverses over all XML files in the ZIP archive.
    pub fn bytes_iter(&mut self) -> ArchiveReaderBytesIter {
        ArchiveReaderBytesIter {
            index: 0,
            end: self.archive.len(),
            archive: &mut self.archive,
        }
    }
}

/// This iterator traverses over all files (bytes) in the ZIP archive.
pub struct ArchiveReaderBytesIter<'a> {
    index: usize,
    end: usize,
    archive: &'a mut ZipArchive<BufReader<File>>,
}

impl<'a> Iterator for ArchiveReaderBytesIter<'a> {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let result = read_contents_of_file(self.archive, self.index);

            self.index += 1;

            Some(result)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let lower_bound = self.end - self.index;
        let upper_bound = lower_bound;
        (lower_bound, Some(upper_bound))
    }
}

/// Read the `index`th file from a ZIP archive.
fn read_contents_of_file(
    archive: &mut ZipArchive<BufReader<File>>,
    index: usize,
) -> Result<Vec<u8>, Error> {
    let mut inner_file = archive.by_index(index)?;
    let mut content = Vec::new();
    inner_file.read_to_end(&mut content)?;
    Ok(content)
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Write;
    use tempfile::{NamedTempFile, TempPath};

    #[test]
    fn read_simple_zip_file() {
        let path = create_zip_file(&[MockFile {
            name: "Test".into(),
            content: "Foobar".into(),
        }]);

        let mut reader = ArchiveReader::from_path(&path).expect("Cannot read file.");
        let mut archive_iter = reader.bytes_iter();

        let file = archive_iter
            .next()
            .expect("Missing first file")
            .expect("Unable to read first file");

        assert_eq!(file, b"Foobar");

        assert!(archive_iter.next().is_none());
    }

    #[test]
    fn read_multiple_files_in_zip_file() {
        let path = create_zip_file(&[
            MockFile {
                name: "Test".into(),
                content: "Foo".into(),
            },
            MockFile {
                name: "Test2".into(),
                content: "Bar".into(),
            },
        ]);

        let mut reader = ArchiveReader::from_path(&path).expect("Cannot read file.");
        let archive_iter = reader.bytes_iter();

        let mut number_of_files = 0;
        let mut contents = Vec::<Vec<u8>>::new();
        for bytes in archive_iter.map(Result::unwrap) {
            number_of_files += 1;
            contents.push(bytes);
        }

        assert_eq!(number_of_files, 2);
        assert_eq!(contents, vec![b"Foo", b"Bar"]);
    }

    struct MockFile {
        pub name: String,
        pub content: String,
    }

    fn create_zip_file(files: &[MockFile]) -> TempPath {
        let mut file = NamedTempFile::new().expect("Unable to create file to test.");

        {
            let mut zip_writer = zip::ZipWriter::new(&mut file);

            let options = zip::write::FileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            for file in files {
                zip_writer
                    .start_file(file.name.as_str(), options)
                    .expect("Unable to start file in zip archive.");
                zip_writer
                    .write_all(file.content.as_bytes())
                    .expect("Unable to write file in zip archive.");
            }

            zip_writer.finish().expect("Unable to finish zip archive.");
        }

        file.into_temp_path()
    }
}
