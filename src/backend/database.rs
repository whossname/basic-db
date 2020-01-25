extern crate serde;
extern crate sysconf;

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

#[derive(Debug)]
pub enum Column {
    Null(),
    Integer(i128),
    Real(f64),
    Blob(Vec<u8>),
    Text(String),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum ColumnType {
    Integer = 1,
    Real = 2,
    Blob = 3,
    Text = 4,
}

impl Database {
    fn read_page(&mut self, page_number: u32) -> Result<Page, Box<dyn error::Error>> {
        let mut page: Vec<u8> = vec![0; self.page_size as usize];

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

    pub fn describe_table(
        &mut self,
        table_name: String,
    ) -> Result<(u32, Vec<(String, ColumnType)>), Box<dyn error::Error>> {
        let record_filter = |row: &Vec<Column>| match &row[1] {
            Column::Text(row_table_name) => *row_table_name == table_name,
            _ => false,
        };

        let column_filter = |mut row: Vec<Column>| row.drain(2..).collect();

        let table = self.select_records(1, record_filter, column_filter)?;
        let columns = table.first().unwrap();

        match columns.as_slice() {
            [Column::Integer(page_number), Column::Blob(data)] => {
                let columns = bincode::deserialize::<Vec<(String, ColumnType)>>(data);
                Ok((*page_number as u32, columns.unwrap()))
            }
            _ => panic!("Table columns stored incorrectly"),
        }
    }

    pub fn select_records<'a, RecF, ColF>(
        &mut self,
        page_number: u32,
        record_filter: RecF,
        mut column_filter: ColF,
    ) -> Result<Vec<Vec<Column>>, Box<dyn error::Error>>
    where
        RecF: Fn(&Vec<Column>) -> bool,
        ColF: FnMut(Vec<Column>) -> Vec<Column>,
    {
        let page = self.read_page(page_number);

        match page {
            Ok(Page {
                page: page_content,
                page_type: PageType::TableLeaf(leaf),
            }) => {
                let mut records: Vec<Vec<Column>> = Vec::new();
                let mut cell_pointer_start = 8;
                if page_number == 1 {
                    cell_pointer_start = 108;
                }

                for cell_count in 0..leaf.cell_count {
                    let cell_pointer_index = (cell_pointer_start + 2 * cell_count) as usize;
                    let slice = &page_content[cell_pointer_index..cell_pointer_index + 2];
                    let cell_pointer: u16 = serialise::to_integer(slice)?;
                    let record = fetch_record(&mut (cell_pointer as usize), &page_content);

                    if record_filter(&record) {
                        let filtered_record = column_filter(record);
                        records.push(filtered_record);
                    }
                }
                // return records

                Ok(records)
            }

            Ok(Page {
                page: page_content,
                page_type: PageType::TableInterior(interior),
            }) => {
                // for each page
                // recursively call self
                // return combined results

                panic!("Not implemented")
            }
            _ => panic!("Not implemented"),
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
        let serialised_columns = bincode::serialize(&columns)?;

        let row = vec![
            Column::Integer(schema_type),
            Column::Text(table_name),
            Column::Integer(rootpage as i128),
            Column::Blob(serialised_columns),
        ];

        let record = create_record(row);
        self.insert_record(record, 1);

        page::table_leaf::create_page(self)?;

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
                // check if there is enough space
                // if not, do we need to split the leaf or add an overflow page?

                let record_size = record.len() as u16;
                let cell_pointer = if leaf.cell_content_start == 0 {
                    self.page_size - record_size
                } else {
                    leaf.cell_content_start - record_size
                };

                // add record
                let cp = cell_pointer as usize;
                let cell_pointer_range = cp..cp + record_size as usize;
                page_content.splice(cell_pointer_range, record);

                // add pointer to record
                let mut cell_pointer_bytes = vec![0u8; 2];
                serialise_integer!(cell_pointer, &mut 0, &mut cell_pointer_bytes);

                let mut page_header_start = 0;
                if page_number == 1 {
                    page_header_start = 100;
                }

                let cell_pointer_offset = leaf.cell_count as usize * 2 + 8 + page_header_start;
                page_content.splice(
                    cell_pointer_offset..cell_pointer_offset + 2,
                    cell_pointer_bytes.clone(),
                );

                // update cell count
                let cell_count = leaf.cell_count + 1;
                let mut cell_count_bytes = vec![0u8; 2];
                serialise_integer!(cell_count, &mut 0, &mut cell_count_bytes);

                let cell_count_offset = page_header_start + 3;
                page_content.splice(cell_count_offset..cell_count_offset + 2, cell_count_bytes);

                // update cell content start
                let cell_content_start_offset = page_header_start + 5;
                page_content.splice(
                    cell_content_start_offset..cell_content_start_offset + 2,
                    cell_pointer_bytes,
                );

                // save changes
                self.save_page(page_content, page_number)
                    .expect("failed to save page");
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

fn fetch_record(cell_pointer: &mut usize, page_content: &Vec<u8>) -> Vec<Column> {
    let cell_start = cell_pointer.clone();
    let header_size = read_varint(cell_pointer, page_content);
    let header_end = header_size as usize + cell_start;

    let mut serial_types: Vec<u64> = Vec::new();
    while *cell_pointer < header_end {
        let serial_type = read_varint(cell_pointer, page_content);
        serial_types.push(serial_type);
    }

    let mut record: Vec<Column> = Vec::new();
    for serial_type in serial_types {
        let column = read_column(serial_type, cell_pointer, page_content);
        record.push(column);
    }

    record
}

fn read_column(serial_type: u64, cell_pointer: &mut usize, page_content: &Vec<u8>) -> Column {
    match serial_type {
        0 => Column::Null(),
        1 => read_integer_column(cell_pointer, page_content, 1),
        2 => read_integer_column(cell_pointer, page_content, 2),
        3 => read_integer_column(cell_pointer, page_content, 3),
        4 => read_integer_column(cell_pointer, page_content, 4),
        5 => read_integer_column(cell_pointer, page_content, 6),
        6 => read_integer_column(cell_pointer, page_content, 8),
        7 => {
            let mut bytes = [0; 8];
            for i in 0..8 {
                bytes[i] = page_content[*cell_pointer + i];
            }

            let val = f64::from_be_bytes(bytes);
            *cell_pointer += 8;
            Column::Real(val)
        }
        8 => Column::Integer(0),
        9 => Column::Integer(1),
        x if x % 2 == 0 => {
            let len = (x - 12) / 2;
            let bytes = read_bytes(cell_pointer, page_content, len as usize);
            Column::Blob(bytes)
        }
        x => {
            let len = (x - 13) / 2;
            let bytes = read_bytes(cell_pointer, page_content, len as usize);
            let text = String::from_utf8(bytes).expect("text stored incorrectly");
            Column::Text(text)
        }
    }
}

fn read_bytes(cell_pointer: &mut usize, page_content: &Vec<u8>, len: usize) -> Vec<u8> {
    let end = *cell_pointer + len;
    let mut bytes = Vec::new();

    while *cell_pointer < end {
        let byte = page_content[*cell_pointer as usize];
        bytes.push(byte);
        *cell_pointer += 1;
    }
    bytes
}

fn read_integer_column(cell_pointer: &mut usize, page_content: &Vec<u8>, size: usize) -> Column {
    let int_end = *cell_pointer + size;
    let mut val = 0;

    while *cell_pointer < int_end {
        val = val << 8;
        val = val + page_content[*cell_pointer as usize] as i128;
        *cell_pointer += 1;
    }

    Column::Integer(val)
}

pub fn create_record(row: Vec<Column>) -> Vec<u8> {
    let data: Vec<(u64, Vec<u8>)> = row
        .into_iter()
        .map(|column| match column {
            Column::Null() => (0, Vec::new()),
            Column::Integer(int) => serialise_record_integer(int),
            Column::Real(real) => (7, real.to_be_bytes().to_vec()),
            Column::Blob(blob) => {
                let blob_type = blob.len() as u64 * 2 + 12;
                (blob_type, blob)
            }
            Column::Text(text) => {
                let text_type = text.len() as u64 * 2 + 13;
                (text_type, text.into_bytes())
            }
        })
        .collect();

    let mut header: Vec<u8> = Vec::new();
    let mut body: Vec<u8> = Vec::new();

    for (serial_type, mut column) in data {
        header.append(&mut build_varint(serial_type));
        body.append(&mut column);
    }

    let header_size = build_varint(1 + header.len() as u64);

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

fn read_varint(cell_pointer: &mut usize, page_content: &Vec<u8>) -> u64 {
    let mut varint = 0u64;
    let mut byte_count = 0;

    loop {
        let byte = page_content[*cell_pointer];
        *cell_pointer += 1;
        byte_count += 1;

        if byte_count == 9 {
            varint = varint << 8;
            varint = varint + byte as u64;
            return varint;
        }

        varint = varint << 7;
        varint = varint + (byte & 0x7F) as u64;

        let continue_flag = (byte & 0x80) as u8;

        if continue_flag == 0x00 {
            return varint;
        }
    }
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

    offset = 100;
    let page_type = 13u8;
    serialise_integer!(page_type, &mut offset, &mut page);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        //.append(true)
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
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        //.append(true)
        .open(file_path)?;

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
    use std::fs;

    #[test]
    #[allow(unused_must_use)]
    fn test_database() {
        // setup
        let filename = "test_database_new.db".to_string();
        let path = Path::new(&filename);
        fs::remove_file(path);

        // test
        let mut database = test_new_database(&filename);
        let metadata = fs::metadata(&filename).unwrap();
        assert_eq!(4096, metadata.len());

        test_create_table(&mut database, 1);
        assert_eq!(2, database.page_count);
        let metadata = fs::metadata(&filename).unwrap();
        assert_eq!(2 * 4096, metadata.len());

        test_create_table(&mut database, 2);
        assert_eq!(3, database.page_count);
        let metadata = fs::metadata(&filename).unwrap();
        assert_eq!(3 * 4096, metadata.len());

        // cleanup
        fs::remove_file(path).expect("Failed to delete file");
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

    fn test_create_table(database: &mut Database, table_number: usize) {
        let mut table_name = "table".to_string();
        table_name.push_str(&table_number.to_string());

        let columns = vec![
            ("count".to_string(), ColumnType::Integer),
            ("name string".to_string(), ColumnType::Text),
        ];

        database
            .create_table(table_name.clone(), columns.clone())
            .unwrap();

        let (page_number, columns_out) = database.describe_table(table_name).unwrap();
        assert_eq!(columns, columns_out);
        assert_eq!(page_number as usize, table_number + 1);
    }
}
