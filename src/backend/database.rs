extern crate sysconf;

use std::convert::TryFrom;
use std::error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
// serialise integer
use serialise;
use std::mem;

#[derive(Debug, PartialEq)]
pub struct Database {
    pub page_size: u16,
    pub page_count: u32,
}

pub fn read_page(file: &mut File, page_number: u32, database: Database) 
    -> Result<Vec<u8>, Box<dyn error::Error>> {
    let mut page = Vec::with_capacity(database.page_size as usize);
    let offset = (page_number - 1) as u64 * database.page_size as u64;
    file.seek(SeekFrom::Start(offset));
    file.read_exact(&mut page)?;

    Ok(page)
}

#[allow(dead_code)]
pub fn new(filename: &String) -> Result<(File, Database), Box<dyn error::Error>> {
    let page_size = u16::try_from(sysconf::page::pagesize())?;
    let page_count: u32 = 1;

    let mut header = [0u8; 100];

    let mut offset = 0;
    serialise_integer!(page_size, &mut offset, &mut header);
    serialise_integer!(page_count, &mut offset, &mut header);

    let path = Path::new(filename);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    file.write_all(&header)?;

    let database = Database{
        page_count: page_count,
        page_size: page_size,
    };

    Ok((file, database))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;

    #[test]
    fn test_new_database() {
        let filename = "test_database_new.db".to_string();
        let (mut file, database) = new(&filename).expect("Error creating a new database file");
        let header: &mut [u8; 100] = &mut [0; 100];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(header).unwrap();

        let page_size: u16 = serialise::to_integer(&header[..2]).unwrap();
        let expected_pagesize = u16::try_from(sysconf::page::pagesize()).unwrap();
        assert_eq!(page_size, expected_pagesize);
        assert_eq!(database.page_size, expected_pagesize);

        let pagecount: u32 = serialise::to_integer(&header[2..6]).unwrap();
        assert_eq!(pagecount, 1);
        assert_eq!(database.page_count, pagecount);

        // cleanup
        let path = Path::new(&filename);
        remove_file(path).expect("Failed to delete file");
    }
}
