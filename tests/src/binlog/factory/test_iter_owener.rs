use common::err::decode_error::ReError;

/// 转移所有权的 Iter
pub struct TestOwenerIter {
    data: Vec<Vec<u8>>
}

impl TestOwenerIter {
    pub fn new(data: Vec<Vec<u8>>) -> Self {
        TestOwenerIter {
            data,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<Vec<u8>, ReError>> {
        TestOwenerIterIterator {
            index: 0,
            items: self.data.clone()
        }
    }
}

pub struct TestOwenerIterIterator {
    index: usize,
    items: Vec<Vec<u8>>
}

impl Iterator for TestOwenerIterIterator {
    type Item = Result<Vec<u8>, ReError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.items.len() {
            None
        } else {
            let item = self.items[self.index].clone();
            self.index += 1;

            Some(Ok(item))
        }
    }
}