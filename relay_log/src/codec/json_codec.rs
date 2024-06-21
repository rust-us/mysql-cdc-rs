use std::fmt::{Debug, Formatter};
use crate::codec::codec::Codec;

#[derive(Clone)]
pub struct JsonCodec {

}

impl Codec for JsonCodec {
    fn new() -> Self where Self: Sized {
        JsonCodec {

        }
    }

    fn name(&self) -> String {
        String::from("JsonCodec")
    }
}

impl Debug for JsonCodec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

