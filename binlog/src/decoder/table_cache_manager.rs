use std::collections::HashMap;
use crate::ast::query_parser::{TableInfo};

#[derive(Debug, Clone)]
pub struct TableCacheManager {
    map: HashMap</*u64*/String, TableInfo>,
}

impl TableCacheManager {
    pub fn new() -> Self {
        TableCacheManager {
            map: HashMap::new(),
        }
    }

    /// 刷新缓存的表信息
    pub fn fresh_table_info(&mut self, table_info: &TableInfo) -> bool {
        let table_name = table_info.get_table_name();
        if table_name.len() <= 0 {
            return false;
        }

        // 判断是否已经缓存
        if self.map.contains_key(&table_name) {
            // 已经存在缓存，则 update
            // todo

            true
        } else {
            // 否则直接 map insert
            self.map.insert(table_name, table_info.clone());

            true
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub fn get(&self, key: &str) -> Option<&TableInfo> {
        self.map.get(key)
    }

    // pub fn rename_table_info(&mut self) -> bool {
    // }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_1() {
        assert_eq!(1, 1);
    }
}