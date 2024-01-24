use common::err::decode_error::ReError;

/// 不转移所有权的 Iter
pub struct TestRefIter {
    data: Vec<Vec<u8>>
}

impl TestRefIter {
    pub fn new(data: Vec<Vec<u8>>) -> Self {
        TestRefIter {
            data,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<&Vec<u8>, ReError>> {
        TestRefIterIterator {
            index: 0,
            items: &self.data
        }
    }
}

pub struct TestRefIterIterator<'a> {
    index: usize,
    items: &'a Vec<Vec<u8>>
}

impl<'a> Iterator for TestRefIterIterator<'a> {
    type Item = Result<&'a Vec<u8>, ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.items.len() {
            None
        } else {
            let item = &self.items[self.index];
            self.index += 1;

            Some(Ok(item))
        }
    }
}