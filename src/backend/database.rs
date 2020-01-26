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

#[derive(Debug)]
pub struct Database {
    pub page_size: u16,
    pub page_count: u32,
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

impl Database {
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

    pub fn save_page(
        &mut self,
        page: Vec<u8>,
        page_number: u32,
    ) -> Result<(), Box<dyn error::Error>> {
        let offset = (page_number - 1) as u64 * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page)?;
        Ok(())
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
        page::table_leaf::create_page(self)?;

        Ok(())
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
        let columns = table.first().unwrap();

        match columns.as_slice() {
            [Column::Integer(page_number), Column::Blob(data)] => {
                let columns = bincode::deserialize::<Vec<(String, ColumnType)>>(data);
                Ok((*page_number as u32, columns.unwrap()))
            }
            _ => panic!("Table columns stored incorrectly"),
        }
    }

    pub fn insert_record(
        &mut self,
        table_name: String,
        row_hash: HashMap<String, String>,
    ) -> Result<(), Box<dyn error::Error>> {
        let (page_number, columns) = self.describe_table(table_name)?;

        let row = columns
            .into_iter()
            .map(|col| match col {
                (col_name, ColumnType::Integer) => {
                    let column_wrapper = |value| Column::Integer(value);
                    parse_column(&row_hash, col_name, column_wrapper)
                }
                (col_name, ColumnType::Real) => {
                    let column_wrapper = |value| Column::Real(value);
                    parse_column(&row_hash, col_name, column_wrapper)
                }
                (col_name, ColumnType::Text) => {
                    let column_wrapper = |value| Column::Text(value);
                    parse_column(&row_hash, col_name, column_wrapper)
                }
                (col_name, ColumnType::Blob) => {
                    let value = row_hash.get(&col_name);
                    match value {
                        Some(value) => {
                            // TODO parse binary correctly
                            let value = value.clone().into_bytes();
                            Column::Blob(value)
                        }
                        None => Column::Null(),
                    }
                }
            })
            .collect();

        let record = record::create_record(row);
        record::insert_record(self, record, page_number);
        Ok(())
    }

    pub fn select_all_records(
        &mut self,
        table_name: String,
    ) -> Result<Vec<Vec<Column>>, Box<dyn error::Error>> {
        let record_filter = |_row: &Vec<Column>| true;
        let column_filter = |row: Vec<Column>| row;
        self.select_records(table_name, record_filter, column_filter)
    }

    pub fn select_records<RecF, ColF>(
        &mut self,
        table_name: String,
        record_filter: RecF,
        column_filter: ColF,
    ) -> Result<Vec<Vec<Column>>, Box<dyn error::Error>>
    where
        RecF: Fn(&Vec<Column>) -> bool,
        ColF: FnMut(Vec<Column>) -> Vec<Column>,
    {
        let (page_number, _columns) = self.describe_table(table_name)?;
        record::select_records(self, page_number, record_filter, column_filter)
    }
}

fn parse_column<T: std::str::FromStr, ColFn>(
    row_hash: &HashMap<String, String>,
    col_name: String,
    column_wrapper: ColFn,
) -> Column
where
    ColFn: Fn(T) -> Column,
{
    let value = row_hash.get(&col_name);
    match value {
        Some(value) => {
            let value = value.parse();
            match value {
                Ok(value) => column_wrapper(value),
                _ => panic!("input value not compatible with data type"),
            }
        }
        None => Column::Null(),
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

        test_create_table(&mut database, 1, &filename);
        test_create_table(&mut database, 2, &filename);

        test_insert_record(&mut database, 1);
        test_insert_record(&mut database, 2);

        // cleanup
        fs::remove_file(path).expect("Failed to delete file");
    }

    fn test_insert_record(database: &mut Database, table_number: usize) {
        let mut table_name = "table".to_string();
        table_name.push_str(&table_number.to_string());

        // build hash
        let name = "fred".to_string();
        let mut row = HashMap::new();
        row.insert("count".to_string(), table_number.to_string());
        row.insert("name".to_string(), name.clone());

        // insert
        database
            .insert_record(table_name.clone(), row)
            .expect("failed to insert record");

        // test select all
        let output = database.select_all_records(table_name.clone()).unwrap();
        let expected = vec![vec![
            Column::Integer(table_number as i128),
            Column::Text(name.clone()),
        ]];
        assert_eq!(output, expected);

        // test select count where name
        let record_filter = |row: &Vec<Column>| match &row[1] {
            Column::Text(val) => *val == name,
            _ => false,
        };

        let column_filter = |mut row: Vec<Column>| row.drain(..1).collect();

        let output = database
            .select_records(table_name, record_filter, column_filter)
            .unwrap();

        let count_out = output.first().unwrap().first().unwrap();
        assert_eq!(*count_out, Column::Integer(table_number as i128));
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

    fn test_create_table(database: &mut Database, table_number: usize, filename: &String) {
        let mut table_name = "table".to_string();
        table_name.push_str(&table_number.to_string());

        let columns = vec![
            ("count".to_string(), ColumnType::Integer),
            ("name".to_string(), ColumnType::Text),
        ];

        database
            .create_table(table_name.clone(), columns.clone())
            .unwrap();

        let (page_number, columns_out) = database.describe_table(table_name).unwrap();
        assert_eq!(columns, columns_out);
        assert_eq!(page_number as usize, table_number + 1);

        assert_eq!(table_number as u32 + 1, database.page_count);
        let metadata = fs::metadata(&filename).unwrap();
        assert_eq!((table_number as u64 + 1) * 4096, metadata.len());
    }
}
