use self::page::Page;
use self::page::PageType;
use super::super::database::{Column, Database};
use super::super::page;
use std::mem;

pub fn insert_record(database: &mut Database, record: Vec<u8>, rootpage: u32) {
    // TODO rootpage is not always page_number
    let page_number = rootpage;

    // check cache
    let mut page = match database.page_cache.remove(&page_number) {
        None => database.read_page(page_number).unwrap(),
        Some(page) => page,
    };

    println!("{:?}", page);

    // find appropriate page for insert

    match page.page_type {
        PageType::TableLeaf(ref mut leaf) => {
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
            page.data.splice(cell_pointer_range, record);

            // add pointer to record
            let mut cell_pointer_bytes = vec![0u8; 2];
            serialise_integer!(cell_pointer, &mut 0, &mut cell_pointer_bytes);

            let mut page_header_start = 0;
            if page_number == 1 {
                page_header_start = 100;
            }

            let cell_pointer_offset = leaf.cell_count as usize * 2 + 8 + page_header_start;
            page.data.splice(
                cell_pointer_offset..cell_pointer_offset + 2,
                cell_pointer_bytes.clone(),
            );

            // update cell count
            let cell_count = leaf.cell_count + 1;
            let mut cell_count_bytes = vec![0u8; 2];
            serialise_integer!(cell_count, &mut 0, &mut cell_count_bytes);

            let cell_count_offset = page_header_start + 3;
            page.data
                .splice(cell_count_offset..cell_count_offset + 2, cell_count_bytes);

            // update cell content start
            let cell_content_start_offset = page_header_start + 5;
            page.data.splice(
                cell_content_start_offset..cell_content_start_offset + 2,
                cell_pointer_bytes,
            );

            leaf.cell_count = cell_count;
            leaf.cell_content_start = cell_pointer;

            database.page_cache.insert(page_number, page);
        }
        PageType::TableInterior(_interior) => {
            // find next page
            // recursively call self

            panic!("Not implemented")
        }
        _ => panic!("Not implemented"),
    };
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

    let mut header_size = header.len() as u64;
    let zeros = header_size.leading_zeros() as u64;

    let header_size_size = if zeros < 8 {
        9u64
    } else {
        // 64 bits offset by -1 because 7 bits needs 1 byte, not 2
        let required_bits = 63 - zeros;
        required_bits / 7 + 1
    };

    header_size = header_size + header_size_size;
    let header_size = build_varint(header_size);

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
