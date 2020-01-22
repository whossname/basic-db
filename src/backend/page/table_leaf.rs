use super::super::database::Database;
use super::{Page, PageType};
use serialise;
use std::error;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::Write;
use std::mem;

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

/*
pub fn add_cell(
    page: PageType,
    database: Database,
    id: u64,
    data: &[u8],
) -> Result<Page, Box<dyn error::Error>> {
    let mut cursor_pos = 0;

    if page.cell_count != 0 {
        // move cursor to last cell
        cursor_pos = (page.cell_count as i64 - 1) * 2;
        database.file.seek(SeekFrom::Current(cursor_pos));

        let buffer: &mut [u8; 2] = &mut [0; 2];
        database.file.read_exact(buffer)?;
        let cell_offset: i64 = serialise::to_integer(&buffer[..])?;

        cursor_pos = cursor_pos + cell_offset;
        database.file.seek(SeekFrom::Current(cell_offset));
    }

    // load pointer array
    // find insert location
    // check last cell
    // binary search the rest
    // add cell pointer

    // add cell data

    Ok(page)
}
*/

#[cfg(test)]
mod tests {
    /*
    use super::*;
    use std::fs::remove_file;
    use std::fs::OpenOptions;
    use std::path::Path;

    use std::fs::File;

    #[test]
    fn test_new_table_leaf() {
        let filename = "test_table_leaf_init.db".to_string();

        let path = Path::new(&filename);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Error opening file");

        create_page(&mut file).expect("Error creating a new database file");

        file.seek(SeekFrom::Start(0))
            .expect("Error resetting file cursor");

        let database = Database {
            page_size: 4098,
            page_count: 1,
            file: file,
        };

        let header = get_header(2, database).expect("Error retrieving header");

        assert_eq!(
            header,
            TableLeaf {
                freeblock_index: 0,
                cell_count: 0,
                cell_content_start: 0,
                fragmented_bytes_count: 0,
            }
        );

        // cleanup
        let path = Path::new(&filename);
        remove_file(path).expect("Failed to delete file");
    }
    */
}
