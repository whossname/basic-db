extern crate serde;
extern crate sysconf;

use self::page::table_leaf::TableLeaf;
use self::page::Page;
use self::page::PageType;
use super::page;
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

#[derive(Debug)]
pub struct Database {
    pub page_size: u16,
    pub page_count: u32,
    pub file: File,
}

pub enum Column {
    Integer(i128),
    Real(f64),
    Blob(Vec<u8>),
    Text(String),
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "t", content = "c")]
#[repr(u8)]
pub enum ColumnType {
    Integer = 1,
    Real = 2,
    Blob = 3,
    Text = 4,
}

impl Database {
    fn read_page(&mut self, page_number: u32) -> Result<Page, Box<dyn error::Error>> {
        let mut page = Vec::with_capacity(self.page_size as usize);
        let offset = (page_number - 1) as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut page)?;

        let mut page_header_start = 0;
        if page_number == 1 {
            page_header_start = 100;
        }

        let slice = &page[page_header_start..page_header_start + 1];
        let page_type: u8 = serialise::to_integer(slice)?;

        match page_type {
            2 => unimplemented!("index_interior"),
            5 => unimplemented!("table_interior"),
            10 => unimplemented!("index_leaf"),
            13 => page::table_leaf::read_page(page, page_header_start),
            _ => panic!("Unknown page type: {}", page_type),
        }
    }

    pub fn create_table(
        &mut self,
        table_name: String,
        columns: Vec<(String, ColumnType)>,
    ) -> Result<(), Box<dyn error::Error>> {
        let rootpage = self.page_count + 1;
        self.page_count = rootpage;
        let schema_type = 1;

        let row = vec![
            Column::Integer(schema_type),
            Column::Text(table_name),
            Column::Integer(rootpage as i128),
            Column::Blob(bincode::serialize(&columns)?),
        ];

        let record = create_record(row);
        self.insert_record(record, 1);

        Ok(())
    }

    fn insert_record(&mut self, record: Vec<u8>, rootpage: u32) {
        // find appropriate page for insert
        let mut page_number = rootpage;

        let page = self.read_page(page_number);

        match page {
            Ok(Page {
                page: mut page_content,
                page_type: PageType::TableLeaf(leaf),
            }) => {
                let record_size = record.len() as u16;

                // check if there is enough space
                // if not, do we need to split the leaf or add an overflow page?

                let cell_pointer = if leaf.cell_content_start == 0 {
                    self.page_size - record_size
                } else {
                    leaf.cell_content_start - record_size
                } as usize;

                // add record
                let cell_pointer_range = cell_pointer..cell_pointer + record_size as usize;
                page_content.splice(cell_pointer_range, record);

                // add pointer to record
                let mut cell_pointer_bytes = vec![0u8; 2];
                serialise_integer!(cell_pointer, &mut 0, &mut cell_pointer_bytes);

                let mut cell_pointer_location = leaf.cell_count as usize * 2 + 8;
                if page_number == 1 {
                    cell_pointer_location += 100;
                }

                page_content.splice(
                    cell_pointer_location..cell_pointer_location + 2,
                    cell_pointer_bytes,
                );

                // save changes
                self.save_page(page_content, page_number);
            }
            Ok(Page {
                page: page_content,
                page_type: PageType::TableInterior(interior),
            }) => {
                // find next page
                // recursively call self

                panic!("Not implemented")
            }
            _ => panic!("Not implemented"),
        }
    }

    fn save_page(&mut self, page: Vec<u8>, page_number: u32) -> Result<(), Box<dyn error::Error>> {
        let offset = (page_number - 1) as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page)?;
        Ok(())
    }
}

pub fn create_record(row: Vec<Column>) -> Vec<u8> {
    let data: Vec<(u64, Vec<u8>)> = row
        .into_iter()
        .map(|column| match column {
            Column::Integer(int) => serialise_record_integer(int),
            Column::Real(real) => (7, real.to_be_bytes().to_vec()),
            Column::Text(text) => {
                let text_type = text.len() as u64 * 2 + 12;
                (text_type, text.into_bytes())
            }
            Column::Blob(blob) => {
                let blob_type = blob.len() as u64 * 2 + 13;
                (blob_type, blob)
            }
        })
        .collect();

    let mut header: Vec<u8> = Vec::new();
    let mut body: Vec<u8> = Vec::new();

    for (serial_type, mut column) in data {
        header.append(&mut build_varint(serial_type));
        body.append(&mut column);
    }

    let header_size = build_varint(header.len() as u64);

    header_size
        .into_iter()
        .chain(header.into_iter())
        .chain(body.into_iter())
        .collect()
}

fn build_varint(int: u64) -> Vec<u8> {
    let mut varint: Vec<u8> = Vec::new();
    let mut val = int;

    if int.leading_zeros() < 8 {
        // use 9 bytes
        let byte = (val & 0xFF) as u8;
        varint.push(byte);
        val = val >> 8;
    } else {
        let byte = (val & 0x7F) as u8;
        varint.push(byte);
        val = val >> 7;
    }

    while val > 0 {
        // add bytes until finished
        let byte = ((val & 0x7F) | 0x80) as u8;
        varint.push(byte);
        val = val >> 7;
    }

    varint.reverse();
    varint
}

// max length of a varint is 9 bytes
fn varint_length(int: i128) -> u8 {
    let mut leading_bits = int.leading_zeros();
    if leading_bits == 0 {
        leading_bits = (!int).leading_zeros();
    }
    (16 - leading_bits / 8) as u8
}

fn serialise_record_integer(int: i128) -> (u64, Vec<u8>) {
    match int {
        0 => return (8, Vec::new()),
        1 => return (9, Vec::new()),
        -1 => return (1, vec![255]),
        _ => (),
    }

    let mut buffer = vec![0u8; 16];
    serialise_integer!(int, &mut 0, &mut buffer);
    let mut leading_bits = int.leading_zeros();
    if leading_bits == 0 {
        leading_bits = (!int).leading_zeros();
    }
    let length = 16 - leading_bits / 8;

    match &length {
        1..=4 => {
            // length == type
            buffer.drain(..16 - length as usize);
            return (length as u64, buffer);
        }
        5 | 6 => {
            // type 5 uses 6 bytes
            buffer.drain(..10);
            return (5, buffer);
        }
        7 | 8 => {
            // type 6 uses 8 bytes
            buffer.drain(..8);
            return (6, buffer);
        }
        _ => {
            panic!("Integer too large to store");
        }
    }
}

fn create_new_database(file_path: &Path) -> Result<Database, Box<dyn error::Error>> {
    let page_size = u16::try_from(sysconf::page::pagesize())?;
    let page_count: u32 = 1;

    let mut page = vec![0u8; page_size as usize];

    let mut offset = 0;
    serialise_integer!(page_size, &mut offset, &mut page);
    serialise_integer!(page_count, &mut offset, &mut page);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)?;

    file.write_all(&page)?;

    let database = Database {
        page_count: page_count,
        page_size: page_size,
        file: file,
    };

    Ok(database)
}

fn load_existing_database(file_path: &Path) -> Result<Database, Box<dyn error::Error>> {
    let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;

    let header: &mut [u8; 100] = &mut [0; 100];

    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(header).unwrap();

    let page_size = serialise::to_integer(&header[..2])?;
    let page_count = serialise::to_integer(&header[2..6])?;

    let database = Database {
        page_count: page_count,
        page_size: page_size,
        file: file,
    };

    Ok(database)
}

pub fn load(filename: &String) -> Result<Database, Box<dyn error::Error>> {
    let file_path = Path::new(filename);

    if file_path.exists() {
        load_existing_database(file_path)
    } else {
        create_new_database(file_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;

    #[test]
    fn test_database() {
        let filename = "test_database_new.db".to_string();
        let mut database = test_new_database(&filename);
        let table = test_create_table(&mut database);

        // cleanup
        let path = Path::new(&filename);
        remove_file(path).expect("Failed to delete file");
    }

    fn test_new_database(filename: &String) -> Database {
        let mut database = load(filename).expect("Error creating a new database file");

        let header: &mut [u8; 100] = &mut [0; 100];
        database.file.seek(SeekFrom::Start(0)).unwrap();
        database.file.read_exact(header).unwrap();

        let page_size: u16 = serialise::to_integer(&header[..2]).unwrap();
        let expected_pagesize = u16::try_from(sysconf::page::pagesize()).unwrap();
        assert_eq!(page_size, expected_pagesize);
        assert_eq!(database.page_size, expected_pagesize);

        let pagecount: u32 = serialise::to_integer(&header[2..6]).unwrap();
        assert_eq!(pagecount, 1);
        assert_eq!(database.page_count, pagecount);

        database
    }
    fn test_create_table(database: &mut Database) {
        let table_name = "table".to_string();
        let columns = vec![
            ("count".to_string(), ColumnType::Integer),
            ("name string".to_string(), ColumnType::Text),
        ];

        database.create_table(table_name, columns);
    }
}
