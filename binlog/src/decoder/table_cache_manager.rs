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

        if self.map.contains_key(&table_name) {
            // update
            // todo

            true
        } else {
            // insert
            self.map.insert(table_name, table_info.clone());

            true
        }
    }

    // pub fn rename_table_info(&mut self) -> bool {
    // }
}