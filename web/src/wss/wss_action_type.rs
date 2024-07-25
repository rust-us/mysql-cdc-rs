use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::web_error::WebError;

#[derive(TryFromPrimitive, IntoPrimitive, Debug, Copy, Clone)]
#[repr(i16)]
#[derive(Eq, PartialEq)]
pub enum ActionType {
    CONNECTION = 0,
    StartBinlog = 1,

    IGNORE = 10,

    Unknown = -1,
}

impl TryFrom<String> for ActionType {
    type Error = WebError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "CONNECTION" => {
                Ok(Self::CONNECTION)
            },
            "StartBinlog" => {
                Ok(Self::StartBinlog)
            },
            "IGNORE" => {
                Ok(Self::IGNORE)
            },
            _ => {
                Ok(Self::Unknown)
            }
        }
    }
}
