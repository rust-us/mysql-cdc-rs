use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::web_error::WebError;

#[derive(TryFromPrimitive, IntoPrimitive, Debug, Copy, Clone)]
#[repr(i16)]
#[derive(Eq, PartialEq)]
pub enum ActionType {
    CONNECTION = 0,

    IGNORE = 1,

    UNKNOW = -1,
}

impl TryFrom<String> for ActionType {
    type Error = WebError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "CONNECTION" => {
                Ok(Self::CONNECTION)
            },
            "IGNORE" => {
                Ok(Self::IGNORE)
            },
            "UNKNOW" => {
                Ok(Self::UNKNOW)
            },
            _ => {
                Ok(Self::UNKNOW)
                // Err(WebError::Value(
                //     format!("unknown ActionType: {}", value)
                // ))
            }
        }
    }
}
