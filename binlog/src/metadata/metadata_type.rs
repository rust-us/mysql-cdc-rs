use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum MetadataType {
    /// UNSIGNED flag of numeric columns
    Signedness = 1,

    /// Default character set of string columns
    DefaultCharset = 2,

    /// Character set of string columns
    ColumnCharset = 3,

    ColumnName = 4,

    /// String value of SET columns
    SetStrValue = 5,

    /// String value of ENUM columns
    EnumStrValue = 6,

    /// Real type of geometry columns
    GeometryType = 7,

    /// Primary key without prefix
    SimplePrimaryKey = 8,

    /// Primary key with prefix
    PrimaryKeyWithPrefix = 9,

    /// Character set of enum and set columns, optimized to minimize space when many columns have the same charset.
    EnumAndSetDefaultCharset = 10,

    /// Character set of enum and set columns, optimized to minimize space when many columns have the same charset.
    EnumAndSetColumnCharset = 11,

    /// Flag to indicate column visibility attribute
    ColumnVisibility = 12,
}

#[cfg(test)]
mod test {
    use crate::metadata::metadata_type::MetadataType;

    #[test]
    fn test() {
        let code = MetadataType::ColumnCharset;
        assert_eq!(3, u8::from(code));

        let t = MetadataType::try_from(6).unwrap();
        assert_eq!(t, MetadataType::EnumStrValue);
    }

    #[test]
    fn test_into() {
        let st: u8 = MetadataType::EnumStrValue.into();
        assert_eq!(st, 6u8);

        let sp = MetadataType::SimplePrimaryKey;
        let sp_val:u8 = sp.into();
        assert_eq!(sp_val, 8u8);

        let code = MetadataType::ColumnCharset;
        let code_val:u8 = code.into();
        assert_eq!(u8::from(code), 3);
        assert_eq!(code_val, 3);
    }

    #[test]
    fn test_try_from() {
        let pk = MetadataType::try_from(9u8);
        assert_eq!(pk, Ok(MetadataType::PrimaryKeyWithPrefix));

        let three = MetadataType::try_from(113u8);
        assert_eq!(
            three.unwrap_err().to_string(),
            "No discriminant in enum `MetadataType` matches the value `113`",
        );
    }
}
