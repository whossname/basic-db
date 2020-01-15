use super::database::Database;
use serialise;
use std::error;
use std::fs::File;
use std::io::prelude::*;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::mem;

#[derive(Debug, PartialEq)]
pub struct TableLeaf {
    page_type: u8,
    freeblock_index: u16,
    cell_count: u16,
    cell_content_start: u16,
    fragmented_bytes_count: u8,
}

pub fn init(file: &mut File) -> Result<(), Box<dyn error::Error>> {
    let mut header = [0u8; 8];
    let page_type: u8 = 13;
    serialise_integer!(page_type, &mut 0, &mut header);
    file.write_all(&header)?;
    Ok(())
}

pub fn get_header(
    file: &mut File,
    page_number: u32,
    database: Database,
) -> Result<TableLeaf, Box<dyn error::Error>> {
    let header: &mut [u8; 8] = &mut [0; 8];
    file.read_exact(header)?;

    let header = TableLeaf {
        page_type: serialise::to_integer(&header[..1])?,
        freeblock_index: serialise::to_integer(&header[1..3])?,
        cell_count: serialise::to_integer(&header[3..5])?,
        cell_content_start: serialise::to_integer(&header[5..7])?,
        fragmented_bytes_count: serialise::to_integer(&header[7..8])?,
    };

    Ok(header)
}

pub fn add_cell(
    file: &mut File,
    header: TableLeaf,
    id: u64,
    data: &[u8],
) -> Result<TableLeaf, Box<dyn error::Error>> {
    let mut cursor_pos = 0;

    if header.cell_count != 0 {
        // move cursor to last cell
        cursor_pos = (header.cell_count as i64 - 1) * 2;
        file.seek(SeekFrom::Current(cursor_pos));

        let buffer: &mut [u8; 2] = &mut [0; 2];
        file.read_exact(buffer)?;
        let cell_offset: i64 = serialise::to_integer(&buffer[..])?;

        cursor_pos = cursor_pos + cell_offset;
        file.seek(SeekFrom::Current(cell_offset));
    }

    // load pointer array
    // find insert location
    // check last cell
    // binary search the rest
    // add cell pointer

    // add cell data

    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;
    use std::fs::OpenOptions;
    use std::path::Path;

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

        init(&mut file).expect("Error creating a new database file");

        file.seek(SeekFrom::Start(0))
            .expect("Error resetting file cursor");

        let database = Database {
            page_size: 4098,
            page_count: 1,
        };

        let header = get_header(&mut file, 2, database).expect("Error retrieving header");

        assert_eq!(
            header,
            TableLeaf {
                page_type: 13,
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
}
