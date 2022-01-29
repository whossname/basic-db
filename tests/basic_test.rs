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
fn basic_database_tests() {
    let filename = create_db_file("basic");

    let mut database = test_new_database(&filename);

    let metadata = fs::metadata(&filename).unwrap();
    assert_eq!(4096, metadata.len());

    test_create_table(&mut database, 1, &filename);
    test_create_table(&mut database, 2, &filename);

    test_insert_record(&mut database, 1);
    test_insert_record(&mut database, 2);

    cleanup(&filename);
}

#[test]
fn single_record() {
    let filename = create_db_file("single_record");
    let mut database = database::load(&filename).expect("Error creating a new database file");
    create_basic_table(&mut database);
    test_insert_record(&mut database, 1);
}

#[test]
fn multiple_records() {
    let filename = create_db_file("multiple_records");
    let mut database = database::load(&filename).expect("Error creating a new database file");
    create_basic_table(&mut database);

    let row_hashs = vec![simple_record("fred", 1), simple_record("george", 2)];

    database.insert_records("table1".to_string(), row_hashs).unwrap();

    let output = database.select_all_records("table1".to_string()).unwrap();

    let expected = vec![
        vec![Column::Integer(1), Column::Text("fred".to_string())],
        vec![Column::Integer(2), Column::Text("george".to_string())],
    ];
    assert_eq!(output, expected);
}

fn simple_record(name: &str, count: i32) -> HashMap<String, String> {
    let mut row = HashMap::new();
    row.insert("count".to_string(), count.to_string());
    row.insert("name".to_string(), name.to_string());
    row
}

fn simple_insert_record(database: &mut Database, name: &str, count: i32) {
    let mut row = HashMap::new();
    row.insert("count".to_string(), count.to_string());
    row.insert("name".to_string(), name.to_string());

    // insert
    database
        .insert_record("table1".to_string(), row)
        .expect("failed to insert record");
}

fn create_basic_table(database: &mut Database) -> () {
    let columns = vec![
        ("count".to_string(), ColumnType::Integer),
        ("name".to_string(), ColumnType::Text),
    ];

    database
        .create_table("table1".to_string(), columns.clone())
        .unwrap();
}

fn create_db_file(version: &str) -> String {
    let filename = format!("test_database_{}.db", version);
    cleanup(&filename);
    return filename;
}

fn cleanup(filename: &String) {
    let path = Path::new(&filename);
    match fs::remove_file(path) {
        Ok(_) => (),
        Err(error) => match error.kind() {
            ErrorKind::NotFound => (),
            other_error => panic!("Problem opening the file: {:?}", other_error),
        },
    }
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
    let mut database = database::load(filename).expect("Error creating a new database file");
    println!("{}", database);

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
