extern crate num;
use self::num::traits::{Num, PrimInt, CheckedShl};
use std::convert::TryFrom;
use std::error;

#[macro_export]
macro_rules! serialise_integer {
    ($source:ident, $index:expr, $destination:expr) => {{
        let bytes = $source.to_be_bytes();
        let size = mem::size_of_val(&$source);
        let range = *$index..*$index + size;
        *$index = *$index + size;
        $destination[range].clone_from_slice(&bytes);
    }};
}

#[allow(dead_code)]
pub fn to_integer<T>(slice: &[u8]) -> Result<T, Box<dyn error::Error>>
where
    T: std::fmt::Debug + CheckedShl + PrimInt + TryFrom<u8> + Num,
{
    let mut out: T = T::zero();

    println!("{:?}", slice);
    for byte in slice {
        out = out.checked_shl(8).unwrap_or(out);
        match T::try_from(*byte) {
            Ok(v) => out = out + v,
            Err(_) => panic!("failed attempt to convert byte to int"),
        }
    }
    Ok(out)
}
