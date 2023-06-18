extern crate basic_db;
extern crate serde;

use basic_db::backend::database;
use basic_db::backend::database::Column;
use basic_db::backend::database::ColumnType;
use basic_db::backend::database::Database;
use basic_db::serialise;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

#[test]
fn multiple_pages() {
    let filename = format!("multiple_pages.db");
    let mut database = setup_database(&filename);

    let table_name = "large_table".to_string();

    let columns = vec![
        ("count".to_string(), ColumnType::Integer),
        ("name".to_string(), ColumnType::Text),
    ];

    database.create_table(table_name.clone(), columns).unwrap();

    let mut row = HashMap::new();
    let name = "row";

    row.insert("name".to_string(), name.to_string());

    for count in 1..256 {
        println!("{:?}", count);
        row.insert("count".to_string(), count.to_string());

        database
            .insert_record(table_name.clone(), row.clone())
            .expect("failed to insert record");
    }

    println!("{}", database);
}

fn setup_database(filename: &String) -> Database {
    let file_path = Path::new(filename);
    let _ = fs::remove_file(file_path);
    let page_size = 170;
    let mut database = database::create_new_database(file_path, page_size)
        .expect("Error creating a new database file");
    println!("{}", database);

    let header: &mut [u8; 100] = &mut [0; 100];
    database.file.seek(SeekFrom::Start(0)).unwrap();
    database.file.read_exact(header).unwrap();

    let header_page_size: u16 = serialise::to_integer(&header[..2]).unwrap();
    assert_eq!(header_page_size, page_size);
    assert_eq!(database.page_size, page_size);

    let pagecount: u32 = serialise::to_integer(&header[2..6]).unwrap();
    assert_eq!(pagecount, 1);
    assert_eq!(database.page_count, pagecount);

    database
}
