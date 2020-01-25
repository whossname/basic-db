use self::page::Page;
use self::page::PageType;
use super::database::{Column, Database};
use super::page;
use std::error;

// serialise integer
use serialise;
use std::mem;

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

pub fn insert_record(database: &mut Database, record: Vec<u8>, rootpage: u32) {
    // find appropriate page for insert

    let mut page_number = rootpage;
    let page = database.read_page(page_number);

    match page {
        Ok(Page {
            page: mut page_content,
            page_type: PageType::TableLeaf(leaf),
        }) => {
            // check if there is enough space
            // if not, do we need to split the leaf or add an overflow page?

            let record_size = record.len() as u16;
            let cell_pointer = if leaf.cell_content_start == 0 {
                database.page_size - record_size
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
            database.save_page(page_content, page_number)
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
    }}
