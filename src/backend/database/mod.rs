extern crate serde;
extern crate sysconf;

use self::page::Page;
use super::page;
use super::record;
use std::collections::HashMap;
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

mod insert;

#[derive(Debug)]
pub struct Database {
    pub page_cache: HashMap<u32, Page>,
    pub page_count: u32,
    pub page_size: u16,
    pub file: File,
}

#[derive(Debug, PartialEq)]
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

impl std::fmt::Display for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "page_size: {}, page_count: {}",
            self.page_size, self.page_count
        )
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Column::Null() => {
                write!(f, "null")
            }
            Column::Integer(i) => {
                write!(f, "int: {}", i)
            }
            Column::Real(r) => {
                write!(f, "real: {}", r)
            }
            Column::Text(s) => {
                write!(f, "text: {}", s)
            }
            Column::Blob(b) => {
                write!(f, "blob ({})", b.len())
            }
        }
    }
}

impl Database {
    pub fn commit(&mut self) -> Result<(), Box<dyn error::Error>> {
        for (page_number, page) in self.page_cache.drain() {
            let offset = (page_number - 1) as u64 * self.page_size as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&page.data)?;
        }

        Ok(())
    }

    pub fn read_page(&mut self, page_number: u32) -> Result<Page, Box<dyn error::Error>> {
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

        let table = record::select_records(self, 1, record_filter, column_filter)?;

        if let Some(columns) = table.first() {
            match columns.as_slice() {
                [Column::Integer(page_number), Column::Blob(data)] => {
                    let columns = bincode::deserialize::<Vec<(String, ColumnType)>>(data);
                    Ok((*page_number as u32, columns.unwrap()))
                }
                _ => panic!("Table columns stored incorrectly"),
            }
        } else {
            panic!("table {} does not exist", &table_name[..])
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

        let record = record::create_record(row);
        record::insert_record(self, record, 1);
        self.commit()?;
        page::table_leaf::create_page(self)?;

        Ok(())
    }
}

pub fn create_new_database(
    file_path: &Path,
    page_size: u16,
) -> Result<Database, Box<dyn error::Error>> {
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
        page_cache: HashMap::new(),
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
        page_cache: HashMap::new(),
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
        let page_size = u16::try_from(sysconf::page::pagesize())?;
        create_new_database(file_path, page_size)
    }
}
