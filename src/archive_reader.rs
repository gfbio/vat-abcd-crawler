use failure::Error;
use std::fs::File;
use std::io::BufReader;
use zip::ZipArchive;
use std::io::Read;
use std::path::Path;

pub struct ArchiveReader {
    archive: ZipArchive<BufReader<File>>,
//    archive_name: String,
}

impl ArchiveReader {
    pub fn from_path(path: &Path) -> Result<Self, Error> {
//        let archive_name = path.display().to_string();

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let archive = ZipArchive::new(reader)?;

        Ok(Self {
            archive,
//            archive_name,
        })
    }

    pub fn bytes_iter(&mut self) -> ArchiveReaderBytesIter {
        ArchiveReaderBytesIter {
            index: 0,
            end: self.archive.len(),
            archive: &mut self.archive,
//            archive_name: &self.archive_name,
        }
    }
}

pub struct ArchiveReaderBytesIter<'a> {
    index: usize,
    end: usize,
    archive: &'a mut ZipArchive<BufReader<File>>,
//    archive_name: &'a str,
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

fn read_contents_of_file(archive: &mut ZipArchive<BufReader<File>>, index: usize) -> Result<Vec<u8>, Error> {
    let mut inner_file = archive.by_index(index)?;
    let mut content = Vec::new();
    inner_file.read_to_end(&mut content)?;
    Ok(content)
}
