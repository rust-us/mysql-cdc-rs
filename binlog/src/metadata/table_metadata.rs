use crate::metadata::default_charset::DefaultCharset;

/// Contains metadata for table columns.
///
/// <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html">See more</a>
#[derive(Clone, Debug)]
pub struct TableMetadata {
    /// Gets signedness of numeric colums.
    pub signedness: Option<Vec<bool>>,

    /// Gets charsets of character columns.
    pub default_charset: Option<DefaultCharset>,

    /// Gets charsets of character columns.
    pub column_charsets: Option<Vec<u32>>,

    /// Gets column names.
    pub column_names: Option<Vec<String>>,

    /// Gets string values of SET columns.
    pub set_string_values: Option<Vec<Vec<String>>>,

    /// Gets string values of ENUM columns
    pub enum_string_values: Option<Vec<Vec<String>>>,

    /// Gets real types of geometry columns.
    pub geometry_types: Option<Vec<u32>>,

    /// Gets primary keys without prefixes.
    pub simple_primary_keys: Option<Vec<u32>>,

    /// Gets primary keys with prefixes.
    pub primary_keys_with_prefix: Option<Vec<(u32, u32)>>,

    /// Gets charsets of ENUM and SET columns.
    pub enum_and_set_default_charset: Option<DefaultCharset>,

    /// Gets charsets of ENUM and SET columns.
    pub enum_and_set_column_charsets: Option<Vec<u32>>,

    /// Gets visibility attribute of columns.
    pub column_visibility: Option<Vec<bool>>,
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}