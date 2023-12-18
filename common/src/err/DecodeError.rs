use crate::err::DecodeError::DecodeError::Incomplete;

#[derive(Debug, Clone)]
pub enum DecodeError {
    Incomplete(Needed),

    /// The parser had an error (recoverable)
    Error(String),

    /// The parser had an unrecoverable error: we got to the right
    /// branch and we know other branches won't work, so backtrack
    /// as fast as possible
    Failure(String),
}

// impl From<Needed> for DecodeError {
//     fn from(value: Needed) -> Self {
//         Incomplete(value)
//     }
// }

/// Contains information on needed data if a parser returned `Incomplete`
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Needed {
    /// Needs more data, but we do not know how much
    Unknown,

    NoEnoughData,

    InvalidUtf8,

    MissingNull,

    InvalidData(String),
}

impl DecodeError {
    pub fn is_error(&self) -> bool {
        print!("a");

        false
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}