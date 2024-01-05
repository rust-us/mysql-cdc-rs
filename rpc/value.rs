use bytes::Bytes;


pub enum Value {
    Boolean(Option<bool>),
    Byte(Option<u8>),
    Bytes(Option<Bytes>),
    Date(Option<u64>),
    Time(Option<u64>),
    DateTime(Option<u64>),
    Timestamp(Option<u64>),
    Decimal(Option<Bytes>),
    Double(Option<f64>),
    Float(Option<f32>),
    Array(Option<(Bytes, u32)>),
    Geo2D(Option<(u64, u64)>),
    Short(Option<i16>),
    Int(Option<i32>),
    Long(Option<i64>),
    Json(Option<Bytes>),
    // Multi,
    Null,
}

macro_rules! def_writer {
    ($name: ident, $type: ty) => {

    };
}

struct Command {
    buffer: Vec<u8>,
}

impl Command {

    fn write_bool(&mut self, v: Option<bool>) {
        self.buffer.push
    }

    fn write_short(&mut self, v: Option<i16>);


}