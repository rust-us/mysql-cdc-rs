use std::fmt;

#[derive(Clone, PartialEq)]
pub struct RowString {
    values: Vec<String>,
}

// impl fmt::Debug for RowString {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         let mut debug = f.debug_struct("Row");
//         for val in self.values.iter() {
//             match *val {
//                 Some(ref val) => {
//                     debug.field("", val);
//                 },
//                 None => {
//                     debug.field("", &"<taken>");
//                 }
//             }
//         }
//         debug.finish()
//     }
// }

impl RowString {

    /// Creates `Row` from values and columns.
    pub fn new_row(values: Vec<String>) -> Self {
        RowString {
            values,
        }
    }

    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the row has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns true if the row has a length of 0.
    pub fn as_slice(&self) -> &[String] {
        self.values.as_slice()
    }

}