use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Flags {
    pub end_of_stmt: bool,
    pub foreign_key_checks: bool,
    pub unique_key_checks: bool,
    pub has_columns: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct ExtraData {
    pub d_type: ExtraDataType,
    pub data: Payload,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum ExtraDataType {
    RW_V_EXTRAINFO_TAG = 0x00,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum Payload {
    ExtraDataInfo {
        length: u8,
        format: ExtraDataFormat,
        payload: String,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
#[repr(u8)]
pub enum ExtraDataFormat {
    NDB = 0x00,
    OPEN1 = 0x40,
    OPEN2 = 0x41,
    MULTI = 0xff,
}
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Row {
    pub null_bit_mask: Vec<u8>,
    pub values: Vec<u8>,
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}