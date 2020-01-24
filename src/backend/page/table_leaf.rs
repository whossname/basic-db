use super::super::database::Database;
use super::{Page, PageType};
use serialise;
use std::error;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;
use std::mem;

#[derive(Debug)]
pub struct TableLeaf {
    pub freeblock_index: u16,
    pub cell_count: u16,
    pub cell_content_start: u16,
    pub fragmented_bytes_count: u8,
}

impl TableLeaf {
    pub fn free_space(self, header_start: usize) -> u16 {
        self.cell_content_start - self.cell_count * 2 - header_start as u16
    }
}

pub fn create_page(database: &mut Database) -> Result<(), Box<dyn error::Error>> {
    let mut page = vec![0u8; database.page_size as usize];
    let page_type: u8 = 13;
    serialise_integer!(page_type, &mut 0, &mut page);
    database.file.seek(SeekFrom::End(0))?;
    database.file.write_all(&page)?;
    Ok(())
}

pub fn read_page(page: Vec<u8>, header_start: usize) -> Result<Page, Box<dyn error::Error>> {
    let table_leaf = TableLeaf {
        freeblock_index: serialise::to_integer(&page[header_start + 1..header_start + 3])?,
        cell_count: serialise::to_integer(&page[header_start + 3..header_start + 5])?,
        cell_content_start: serialise::to_integer(&page[header_start + 5..header_start + 7])?,
        fragmented_bytes_count: serialise::to_integer(&page[header_start + 7..header_start + 8])?,
    };

    Ok(Page {
        page_type: PageType::TableLeaf(table_leaf),
        page: page,
    })
}
