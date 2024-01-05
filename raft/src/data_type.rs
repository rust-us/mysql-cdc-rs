use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(IntoPrimitive, TryFromPrimitive, Copy, Clone, Debug)]
#[repr(i32)]
pub enum DataType {
    Null = 0,
    Boolean = 1,
    Byte = 2,
    Short = 3,
    Int = 4,
    Long = 5,
    Decimal = 6,
    Double = 7,
    Float = 8,
    Time = 9,
    Date = 10,
    Timestamp = 11,
    Bytes = 12,
    String = 13,
    DateTime = 14,
    // Deprecated
    Other = 19,
    // Deprecated
    MultiValue = 22,
    // Deprecated, use GeoJSON instead
    Geo2D = 23,
    Blob = 24,
    Binary = 25,

    ByteArray = 26,
    ShortArray = 27,
    IntArray = 28,
    FloatArray = 29,
    JSON = 30,
    StringArray = 31,
    DoubleArray = 32,
    LongArray = 33,

    JSONB = 40,
    GeoJSON = 41,
    Geometry = 42,
    Bitmap = 43,
    // Deprecated, not implement
    Map = 50,
}