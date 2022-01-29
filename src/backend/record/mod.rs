use super::database::{Column, Database};
use std::error;

// serialise integer

mod insert;
mod read;

pub fn select_records<'a, RecF, ColF>(
    database: &mut Database,
    page_number: u32,
    record_filter: RecF,
    mut column_filter: ColF,
) -> Result<Vec<Vec<Column>>, Box<dyn error::Error>>
where
    RecF: Fn(&Vec<Column>) -> bool,
    ColF: FnMut(Vec<Column>) -> Vec<Column>,
{
    read::select_records(database, page_number, record_filter, column_filter)
}

pub fn create_record(row: Vec<Column>) -> Vec<u8> {
    insert::create_record(row)
}

pub fn insert_record(database: &mut Database, record: Vec<u8>, rootpage: u32) {
    insert::insert_record(database, record, rootpage)
}

// max length of a varint is 9 bytes
fn varint_length(int: i128) -> u8 {
    let mut leading_bits = int.leading_zeros();
    if leading_bits == 0 {
        leading_bits = (!int).leading_zeros();
    }
    (16 - leading_bits / 8) as u8
}
