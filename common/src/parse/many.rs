use crate::err::decode_error::Needed;
use crate::parse::parse::{InputBuf};

pub struct MB {
    pub age: u8,
    pub ty: u16,
}

pub trait Decode<I: InputBuf, O, E> {
    fn decode(input: &mut I) -> Result<O, E>;
}

impl<I: InputBuf> Decode<I, (), Needed> for u8 {
    fn decode(input: &mut I) -> Result<(), Needed> {
        input.read_u8_ne()?;
        Ok(())
    }
}

impl<I: InputBuf> Decode<I, Self, Needed> for u8 {
    fn decode(input: &mut I) -> Result<Self, Needed> {
        input.read_u8_ne()
    }
}

#[cfg(test)]
mod test {
    use crate::parse::many::Decode;

    #[test]
    fn test_base() {
        assert_eq!(1, 1);

        let arr = [1u8; 1];
        let s: u8 = u8::decode(&mut (&arr[..], ())).unwrap();
        dbg!(s);
    }
}