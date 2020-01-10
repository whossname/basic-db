extern crate num;
extern crate sysconf;

use self::num::traits::{Num, PrimInt};
use std::convert::TryFrom;
use std::error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::mem::transmute;
use std::path::Path;

pub fn new(filename: &String) -> Result<File, Box<dyn error::Error>> {
    let pagesize = u16::try_from(sysconf::page::pagesize())?;
    let page_count: u32 = 1;

    //let mut header_buf = vec![];
    let mut header = [0u8; 100];

    unsafe {
        let pagesize_iter: [u8; 2] = transmute(pagesize.to_be());
        let pagecount_iter: [u8; 4] = transmute(page_count.to_be());

        header[..2].clone_from_slice(&pagesize_iter);
        header[2..6].clone_from_slice(&pagecount_iter);
    };

    //header_buf.write_u16::<BigEndian>(pagesize).unwrap();
    //header_buf.write_u32::<BigEndian>(page_count).unwrap();

    //header.copy_from_slice(&header_buf);

    let path = Path::new(filename);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    file.write_all(&header)?;

    Ok(file)
}

pub fn deserialise<T>(slice: &[u8]) -> Result<T, Box<dyn error::Error>>
where
    T: PrimInt + TryFrom<u8> + Num,
{
    let mut out: T = T::zero();
    for byte in slice {
        out = out << 8;
        match T::try_from(*byte) {
            Ok(v) => out = out + v,
            Err(_) => panic!("failed attempt to convert byte to int"),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_database() {
        let filename = "unit_test_file.db".to_string();
        let mut file = new(&filename).expect("Error creating a new database file");
        let header: &mut [u8; 100] = &mut [0; 100];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(header).unwrap();

        let pagesize: u16 = deserialise(&header[..2]).unwrap();
        let expected_pagesize = u16::try_from(sysconf::page::pagesize()).unwrap();
        assert_eq!(pagesize, expected_pagesize);

        let pagecount: u32 = deserialise(&header[2..6]).unwrap();
        assert_eq!(pagecount, 1)
    }
}
