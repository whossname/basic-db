use serialise;
use std::error;
use std::fs::File;
use std::io::Write;
use std::mem;

pub fn init(file: &mut File) -> Result<(), Box<dyn error::Error>> {
    let mut header = [0u8; 8];

    let page_type: u8 = 13;
    //let freeblock_index: u16 = 0;
    //let cell_count: u16 = 0;
    //let cell_content_start: u16 = 0;
    //let fragmented_bytes_count: u8 = 0;

    let mut offest = 0;
    serialise_integer!(page_type, &mut offest, &mut header);
    //serialise_integer!(freeblock_index, &mut offest, &mut header);
    //serialise_integer!(cell_count, &mut offest, &mut header);
    //serialise_integer!(cell_content_start, &mut offest, &mut header);
    //serialise_integer!(fragmented_bytes_count, &mut offest, &mut header);

    file.write_all(&header)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::OpenOptions;
    use std::io::prelude::*;
    use std::io::Read;
    use std::io::SeekFrom;
    use std::path::Path;
    use std::fs::remove_file;

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

        let header: &mut [u8; 8] = &mut [0; 8];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(header).unwrap();

        let page_type: u8 = serialise::to_integer(&header[..1]).unwrap();
        println!("{:?}", "here");
        let freeblock_index: u16 = serialise::to_integer(&header[1..3]).unwrap();
        let cell_count: u16 = serialise::to_integer(&header[3..5]).unwrap();
        let cell_content_start: u16 = serialise::to_integer(&header[5..7]).unwrap();
        let fragmented_bytes_count: u8 = serialise::to_integer(&header[7..8]).unwrap();

        assert_eq!(page_type, 13);
        assert_eq!(freeblock_index, 0);
        assert_eq!(cell_count, 0);
        assert_eq!(cell_content_start, 0);
        assert_eq!(fragmented_bytes_count, 0);

        // cleanup
        let path = Path::new(&filename);
        remove_file(path).expect("Failed to delete file");
    }
}
