use serde::Serialize;

/// Represents charsets of character columns.
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct DefaultCharset {
    /// Gets the most used charset collation.
    pub default_charset: u32,

    /// Gets ColumnIndex-Charset map for columns that don't use the default charset.
    pub charset_collations: Vec<(u32, u32)>,
}

impl DefaultCharset {
    pub fn new(default_charset: u32, charset_collations: Vec<(u32, u32)>) -> Self {
        Self {
            default_charset,
            charset_collations,
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
