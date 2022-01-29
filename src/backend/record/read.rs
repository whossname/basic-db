use self::page::Page;
use self::page::PageType;
use super::super::database::{Column, Database};
use super::super::page;
use std::error;

use serialise;

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
    let page = database.read_page(page_number);

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
