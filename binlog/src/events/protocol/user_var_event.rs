use std::collections::HashMap;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use common::err::decode_error::ReError;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::declare::log_event::LogEvent;
use crate::events::event_header::Header;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::UserVarType;
use crate::utils::{read_string};

/// A USER_VAR_EVENT is written every time a statement uses a user defined variable.
/// <a href="https://mariadb.com/kb/en/user_var_event/">See more</a>
#[derive(Debug, Serialize, Clone)]
pub struct UserVarEvent {
    header: Header,

    name_length: u32,

    /// User variable name
    pub name: String,

    /// User variable value
    pub value: Option<VariableValue>,
}

/// User variable value
#[derive(Debug, Serialize, Clone)]
pub struct VariableValue {
    pub is_null: bool,

    /// Variable type
    pub var_type: Option<u8>,
    pub d_type: Option<UserVarType>,

    /// Collation number, charset
    pub collation: Option<u32>,

    /// User variable value
    pub value_length: Option<u32>,
    /// User variable value
    pub value: Option<String>,

    /// flags
    pub flags: Option<u8>,
}

impl UserVarEvent {

}

impl LogEvent for UserVarEvent {
    fn get_type_name(&self) -> String {
        "UserVarEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    /// Supports all versions of MariaDB and MySQL.
    fn parse(
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<UserVarEvent, ReError> where Self: Sized {
        let name_len = cursor.read_u32::<LittleEndian>()?;
        let name = read_string(cursor, name_len as usize)?;

        let is_null = cursor.read_u8()? != 0; // 0 indicates there is a value;
        if is_null {
            let checksum = cursor.read_u32::<LittleEndian>()?;
            header.borrow_mut().update_checksum(checksum);

            return Ok(Self {
                header: Header::copy(header.clone()),
                name_length: name_len,
                name,
                value: Some(VariableValue {
                    is_null,
                    var_type: None,
                    d_type: None,
                    collation: None,
                    value_length: None,
                    value: None,
                    flags: None,
                }),
            });
        }

        let var_type = cursor.read_u8()?;
        let d_type = match var_type {
            0 => Some(UserVarType::STRING),
            1 => Some(UserVarType::REAL),
            2 => Some(UserVarType::INT),
            3 => Some(UserVarType::ROW),
            4 => Some(UserVarType::DECIMAL),
            5 => Some(UserVarType::VALUE_TYPE_COUNT),
            _ => Some(UserVarType::Unknown),
        };
        let collation = cursor.read_u32::<LittleEndian>()?;

        let value_len = cursor.read_u32::<LittleEndian>()?;
        let value = read_string(cursor, value_len as usize)?;

        let flags = match d_type.clone().unwrap() {
            UserVarType::INT => {
                let flags = cursor.read_u8()?;
                Some(flags)
            }
            _ => None,
        };

        let checksum = cursor.read_u32::<LittleEndian>()?;
        header.borrow_mut().update_checksum(checksum);

        Ok(Self {
            header: Header::copy(header.clone()),
            name_length: name_len,
            name,
            value: Some(VariableValue {
                is_null,
                var_type: Some(var_type),
                d_type,
                collation: Some(collation),
                value_length: Some(value_len),
                value: Some(value),
                flags,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use crate::events::declare::log_event::LogEvent;
    use crate::events::event_raw::HeaderRef;
    use crate::events::log_context::LogContextRef;
    use crate::events::protocol::user_var_event::UserVarEvent;

    #[test]
    fn parse_user_var_event() {
        let payload: Vec<u8> = vec![
            0x03, 0x00, 0x00, 0x00, 0x66, 0x6f, 0x6f, 0x00, 0x00, 0x21, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x62, 0x61, 0x72, 0x6b, 0x3d, 0xd9, 0x7d, 0x7d,
        ];
        let mut cursor = Cursor::new(payload.as_slice());

        let event = UserVarEvent::parse(&mut cursor, HeaderRef::default(), LogContextRef::default(), None, None).unwrap();
        assert_eq!(String::from("foo"), event.name);
        assert_eq!(false, event.value.is_none());

        let variable = event.value.unwrap();
        assert_eq!(0, variable.var_type.unwrap());
        assert_eq!(33, variable.collation.unwrap());
        assert_eq!(String::from("bar"), variable.value.unwrap());
    }
}
