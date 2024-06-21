use serde::Serialize;
use sqlparser::ast::{AlterTableOperation, ColumnDef, Ident, Statement};
use sqlparser::dialect::{GenericDialect};
use sqlparser::dialect::{MySqlDialect};
use sqlparser::parser::Parser;
use tracing::{debug, error};
use common::binlog::column::column_type::SrcColumnType;
use common::err::CResult;
use crate::ext::sqlparser_ext::sqlparser_data_type_from;

pub struct QueryParser {
    query: String
}

impl QueryParser {
    pub fn new(query: String) -> Self {
        QueryParser {
            query,
        }
    }

    /// 解析 sql, 分析行为。
    pub fn parser_ddl_table_format(&self) -> CResult<Option<TableInfo>> {
        let ddl_sql = &self.query;
        // 只处理部分SQL。 考虑到性能，不全部走AST
        if !ddl_sql.contains("CREATE TABLE") &&
            !ddl_sql.contains("ALTER TABLE") {
            return Ok(None);
        }

        // parse to a Vec<Statement>
        let result = Parser::parse_sql(&MySqlDialect{}, ddl_sql);
        if result.is_err() {
            error!("QueryEventParser, sql [{:?}] parser is unsupport!", ddl_sql);
            return Ok(None);
        }

        let parser = result.unwrap();
        assert_eq!(parser.len(), 1);

        let ast = parser.get(0);

        // The original SQL text can be generated from the AST
        let table_info_build = match ast {
            None => {
                DDLTableInfoBuilder::new(ddl_sql.to_string(), TableFromType::NONE, None).build()
            },
            Some(stat) => {
                self.parser_stat(stat, ddl_sql.to_string())
            }
        };

        Ok(Some(table_info_build.build()))
    }

    /// 解析 Statement
    ///  todo 更多场景未完成
    fn parser_stat(&self, stat: &Statement, ddl_sql:String) -> TableInfoBuilder {
        let table_info_build = match stat {
            Statement::CreateTable { name, columns, .. } => {
                let once_table_name = if name.0.is_empty() {
                    None
                } else {
                    let t = match name.0.get(0) {
                        None => { Ident::new("") }
                        Some(x) => { x.clone() }
                    };
                    Some(t.value.to_string())
                };

                DDLTableInfoBuilder::new(ddl_sql, TableFromType::CREATE, once_table_name).with_add_column_list(columns).build()
            },
            Statement::AlterTable { operations, .. } => {
                let mut info = DDLTableInfoBuilder::new(ddl_sql, TableFromType::ALTER, None);
                for operation in operations {
                    match operation {
                        AlterTableOperation::AddColumn { column_def, .. } => {
                            info.insert_add_column(column_def);
                        }
                        AlterTableOperation::DropColumn { column_name, .. } => {
                            info.insert_remove_column_name(column_name.value.to_string());
                        }
                        AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                            // todo
                        }
                        AlterTableOperation::ChangeColumn { old_name, new_name, .. } => {
                            // todo
                        }
                        AlterTableOperation::AlterColumn { column_name, op } => {
                            // todo
                        }
                        _ => {}
                    }
                };

                info.build()
            },
            _ => {
                // todo TableFromType::ALTER or  TableFromType::DROP
                DDLTableInfoBuilder::new(ddl_sql, TableFromType::UNKNOW, None).build()
            },
        };

        table_info_build
    }
}

struct DDLTableInfoBuilder {
    ddl_sql: String,

    from: TableFromType,

    table_name: Option<String>,

    add_column: Option<Vec<ColumnDef>>,

    remove_column_names: Option<Vec<String>>,
}

impl DDLTableInfoBuilder {
    fn new(ddl_sql: String, from: TableFromType, table_name: Option<String>) -> Self {
        DDLTableInfoBuilder::new_with_columns(ddl_sql, from, table_name, None, None)
    }

    fn new_with_columns(ddl_sql: String, from: TableFromType, table_name: Option<String>, add_column: Option<Vec<ColumnDef>>, remove_column_names: Option<Vec<String>>) -> Self {
        DDLTableInfoBuilder {
            ddl_sql,
            from,
            table_name,
            add_column,
            remove_column_names,
        }
    }

    fn with_add_column(mut self, column: &ColumnDef) -> Self {
        &self.insert_add_column(column);

        self
    }

    fn insert_add_column(&mut self, column: &ColumnDef) {
        if self.add_column.is_none() {
            let mut vec = Vec::<ColumnDef>::new();
            vec.push(column.clone());
            self.add_column = Some(vec);
        } else {
            self.add_column.as_mut().unwrap().push(column.clone());
        }
    }

    fn with_add_column_list(mut self, columns: &Vec<ColumnDef>) -> Self {
        if self.add_column.is_none() {
            let mut vec = Vec::<ColumnDef>::new();

            for column in columns {
                vec.push(column.clone());
            }
            self.add_column = Some(vec);
        } else {
            for column in columns {
                self.add_column.as_mut().unwrap().push(column.clone());
            }
        }

        self
    }

    fn with_remove_column_name(mut self, remove_column_name: String) -> Self {
        &self.insert_remove_column_name(remove_column_name);

        self
    }

    fn insert_remove_column_name(&mut self, remove_column_name: String) {
        if self.remove_column_names.is_none() {
            let mut vec = Vec::<String>::new();
            vec.push(remove_column_name.clone());
            self.remove_column_names = Some(vec);
        } else {
            self.remove_column_names.as_mut().unwrap().push(remove_column_name.clone());
        }
    }

    fn build(&self) -> TableInfoBuilder {
        let mut add_columns = None;
        if self.add_column.is_some() {
            let mut columns = Vec::<ColumnInfo>::new();

            let mut index = 0;
            for column_ref in self.add_column.as_ref().unwrap() {
                // sqlparser_data_type_from 内部实现支持全部转换，因此可以直接安全的 unwrap()
                let (column_type, meta) = sqlparser_data_type_from(column_ref.data_type.clone()).unwrap();
                columns.push(ColumnInfo::new(index, column_ref.name.value.clone(), column_type, meta));
                index += 1;
            }

            add_columns = Some(columns);
        }

        let mut remove_column_names = None;
        if self.remove_column_names.is_some() {
            let mut remove_columns = Vec::<String>::new();

            for remove_column_name in self.remove_column_names.as_ref().unwrap() {
                remove_columns.push(remove_column_name.to_string());
            }

            remove_column_names = Some(remove_columns);
        }

        TableInfoBuilder::new(self.ddl_sql.to_string(), self.from.clone(), self.table_name.clone(),
                              add_columns, remove_column_names)
    }
}


#[derive(Debug, Serialize, Clone)]
pub struct TableInfoBuilder {
    ddl_sql: String,

    from: TableFromType,

    table_name: Option<String>,

    /// 新增的列名
    add_columns: Option<Vec<ColumnInfo>>,

    /// 移除的列名
    remove_columns: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct TableInfo {
    /// SQL
    sql: String,

    from: TableFromType,

    /// 表名
    table_name: String,

    /// 列名。顺序与建表顺序一致
    columns: Option<Vec<ColumnInfo>>,

}

#[derive(Debug, Serialize, Clone)]
pub enum TableFromType {
    CREATE,

    /// contains rename
    ALTER,

    DROP,

    UNKNOW,
    NONE,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct ColumnInfo {
    index: u8,

    name: String,

    column_type: SrcColumnType,

    /// 列的元数据信息.
    ///
    /// 1. 精度信息可以通过 get_scale(meta) 求解
    meta: Option<u16>,
}

impl TableInfo {
    pub fn get_table_name(&self) -> String {
        self.table_name.clone()
    }

    pub fn get_from(&self) -> TableFromType {
        self.from.clone()
    }

    pub fn get_columns(&self) -> Option<&Vec<ColumnInfo>> {
        self.columns.as_ref()
    }
}

impl TableInfoBuilder {
    fn new(ddl_sql: String, from: TableFromType, table_name: Option<String>, add_columns: Option<Vec<ColumnInfo>>, remove_columns: Option<Vec<String>>) -> Self {
        TableInfoBuilder {
            ddl_sql,
            from,
            table_name,
            add_columns,
            remove_columns,
        }
    }

    fn build(&self) -> TableInfo {
        // todo 逻辑
        let columns = self.add_columns.clone();

        TableInfo {
            sql: self.ddl_sql.to_string(),
            from: self.from.clone(),
            // TableInfo 中， table_name 一定存在。
            table_name: self.table_name.clone().unwrap_or_default(),
            columns,
        }
    }
}

impl ColumnInfo {
    fn new(index: u8, name: String, column_type: SrcColumnType, meta: Option<u16>) -> Self {
        ColumnInfo {
            index,
            name,
            column_type,
            meta,
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_1() {
        assert_eq!(1, 1);
    }
}