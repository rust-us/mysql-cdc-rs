use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use tracing::warn;

use binlog::events::binlog_event::BinlogEvent;
use binlog::events::declare::log_event::LogEvent;
use binlog::events::declare::rows_log_event::RowsLogEvent;
use binlog::events::protocol::table_map_event::ColumnInfo;
use binlog::row::row_data::RowData;
use common::binlog::column::column_type::SrcColumnType;
use common::binlog::column::column_value::SrcColumnValue;
use common::binlog::src_meta::SrcType;
use common::schema::data_type::{DstColumnType, Value};

/// 中继日志信息
#[derive(Serialize, Deserialize, Debug, Clone, Getters, Setters)]
pub struct RelayLog {
    /// src type
    #[getset(get = "pub", set = "pub")]
    src_type: SrcType,

    /// binlog event position
    #[getset(get = "pub", set = "pub")]
    event_log_pos: u64,

    /// binlog event name
    #[getset(get = "pub", set = "pub")]
    event_name: String,

    /// database
    #[getset(get = "pub", set = "pub")]
    database_name: String,

    /// table
    #[getset(get = "pub", set = "pub")]
    table_name: String,

    /// column info
    #[getset(get = "pub", set = "pub")]
    columns: Vec<RelayColumnInfo>,

    /// replay command
    #[getset(get = "pub", set = "pub")]
    relay_command: RelayCommand,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RelayCommand {
    None,
    CreateDatabase,
    DropDatabase,
    CreateTable,
    DropTable,
    AlterTable,
    /// 插入数据
    Insert(Vec<RelayRowData>),
    /// 删除数据
    Delete(Vec<RelayRowData>),
    /// 更新数据: (deleteRows, insertRows)
    Update(Vec<(RelayRowData, RelayRowData)>),
}

/// 列信息
#[derive(Serialize, Deserialize, Debug, Clone, Getters, Setters)]
pub struct RelayColumnInfo {
    /// 列类型
    #[getset(get = "pub", set = "pub")]
    column_type: DstColumnType,

    /// 列名称
    #[getset(get = "pub", set = "pub")]
    column_name: String,
}

impl RelayColumnInfo {
    fn from_binlog_column_info(binlog_column_info: &ColumnInfo) -> Self {
        let column_name = binlog_column_info.get_name();
        if let Some(binlog_column_type) = binlog_column_info.get_c_type().take() {
            let column_type = match binlog_column_type {
                SrcColumnType::Null => {
                    DstColumnType::Null
                }
                // Decimal
                SrcColumnType::Decimal |
                SrcColumnType::NewDecimal => {
                    DstColumnType::Decimal
                }
                // Int
                SrcColumnType::Tiny |
                SrcColumnType::Int24 |
                SrcColumnType::Year => {
                    DstColumnType::Int
                }
                SrcColumnType::Short => {
                    DstColumnType::Short
                }
                // Long
                SrcColumnType::Long |
                SrcColumnType::LongLong => {
                    DstColumnType::Long
                }
                SrcColumnType::Float => {
                    DstColumnType::Float
                }
                SrcColumnType::Double => {
                    DstColumnType::Double
                }
                // Timestamp
                SrcColumnType::Timestamp |
                SrcColumnType::Timestamp2 => {
                    DstColumnType::Timestamp
                }
                // Date
                SrcColumnType::Date |
                SrcColumnType::NewDate => {
                    DstColumnType::Date
                }
                // Time
                SrcColumnType::Time |
                SrcColumnType::Time2 => {
                    DstColumnType::Time
                }
                // DateTime
                SrcColumnType::DateTime |
                SrcColumnType::DateTime2 => {
                    DstColumnType::DateTime
                }
                // String
                SrcColumnType::VarChar |
                SrcColumnType::Enum |
                SrcColumnType::Set |
                SrcColumnType::VarString |
                SrcColumnType::String |
                SrcColumnType::Array => {
                    DstColumnType::String
                }
                SrcColumnType::Bit => {
                    DstColumnType::Bitmap
                }
                SrcColumnType::Bool => {
                    DstColumnType::Boolean
                }
                SrcColumnType::Json => {
                    DstColumnType::JSON
                }
                // Blob
                SrcColumnType::TinyBlob |
                SrcColumnType::MediumBlob |
                SrcColumnType::LongBlob |
                SrcColumnType::Blob => {
                    DstColumnType::Blob
                }
                SrcColumnType::Geometry => {
                    DstColumnType::Geometry
                }
                // todo 未知？
                SrcColumnType::Invalid => {
                    DstColumnType::Other
                }
            };
            Self {
                column_type,
                column_name,
            }
        } else {
            Self::default()
        }
    }
}

impl Default for RelayColumnInfo {
    fn default() -> Self {
        Self {
            column_type: DstColumnType::Null,
            column_name: "".to_string(),
        }
    }
}

/// 一行数据
#[derive(Serialize, Deserialize, Debug, Clone, Getters, Setters)]
pub struct RelayRowData {
    #[getset(get = "pub", set = "pub")]
    values: Vec<Value>,
}

impl RelayRowData {
    fn from_binlog_row(binlog_row: &RowData) -> Self {
        let values: Vec<Value> = binlog_row.get_cells().iter().map(|c| {
            if let Some(v) = c {
                match v {
                    // `TinyInt`,`SmallInt`,`MediumInt`,`Int` => `Int`
                    SrcColumnValue::TinyInt(data) => {
                        Value::Int(*data as i32)
                    }
                    SrcColumnValue::SmallInt(data) => {
                        Value::Int(*data as i32)
                    }
                    SrcColumnValue::MediumInt(data) => {
                        Value::Int(*data as i32)
                    }
                    SrcColumnValue::Int(data) => {
                        Value::Int(*data as i32)
                    }
                    // `BigInt` => `Long`
                    SrcColumnValue::BigInt(data) => {
                        Value::Long(*data as i64)
                    }
                    SrcColumnValue::Float(data) => {
                        Value::Float(*data)
                    }
                    SrcColumnValue::Double(data) => {
                        Value::Double(*data)
                    }
                    SrcColumnValue::Decimal(data) => {
                        Value::Decimal(data.to_string())
                    }
                    SrcColumnValue::String(data) => {
                        Value::String(data.to_string())
                    }
                    SrcColumnValue::Blob(data) => {
                        Value::Blob(data.to_vec())
                    }
                    // Year => Int
                    SrcColumnValue::Year(data) => {
                        Value::Int(*data as i32)
                    }
                    SrcColumnValue::Date(data) => {
                        if let Some(naive_date) = NaiveDate::from_ymd_opt(data.year as i32, data.month as u32, data.day as u32) {
                            if let Some(naive_datetime) = naive_date.and_hms_milli_opt(0, 0, 0, 0) {
                                Value::Date(naive_datetime.timestamp_millis())
                            } else {
                                warn!("Date parse error.");
                                Value::Null
                            }
                        } else {
                            warn!("Date parse error.");
                            Value::Null
                        }
                    }
                    SrcColumnValue::Time(data) => {
                        if let Some(naive_time) = NaiveTime::from_hms_milli_opt(data.hour as u32, data.minute as u32, data.second as u32, data.millis) {
                            let now_date = Utc::now().date_naive();
                            Value::Time(NaiveDateTime::new(now_date, naive_time).timestamp_millis())
                        } else {
                            warn!("Time parse error.");
                            Value::Null
                        }
                    }
                    SrcColumnValue::DateTime(data) => {
                        if let Some(naive_date) = NaiveDate::from_ymd_opt(data.year as i32, data.month as u32, data.day as u32) {
                            if let Some(naive_datetime) = naive_date.and_hms_milli_opt(data.hour as u32, data.minute as u32, data.second as u32, data.millis) {
                                Value::DateTime(naive_datetime.timestamp_millis())
                            } else {
                                warn!("DateTime parse error.");
                                Value::Null
                            }
                        } else {
                            warn!("DateTime parse error.");
                            Value::Null
                        }
                    }
                    SrcColumnValue::Timestamp(data) => {
                        Value::Timestamp(*data as i64)
                    }

                    // todo 暂不支持的类型：`Bit`,`Enum`,`Set`
                    SrcColumnValue::Bit(data) => {
                        Value::Null
                    }
                    SrcColumnValue::Enum(data) => {
                        Value::Null
                    }
                    SrcColumnValue::Set(data) => {
                        Value::Null
                    }
                }
            } else {
                Value::Null
            }
        }).collect();

        Self {
            values
        }
    }
}

impl Default for RelayRowData {
    fn default() -> Self {
        Self {
            values: vec![]
        }
    }
}

impl RelayLog {
    pub fn from_binlog_event(event: &BinlogEvent) -> Self {
        // todo 暂时写死 Mysql 源
        let src_type = SrcType::Mysql;
        match event {
            BinlogEvent::WriteRows(e) => {
                if let Some(table) = e.get_table_map_event() {
                    let event_log_pos = e.get_header().get_log_pos();
                    let event_name = e.get_type_name();
                    let insert_rows: Vec<RelayRowData> = e.get_rows()
                        .iter()
                        .map(|r| {
                            RelayRowData::from_binlog_row(r)
                        }).collect();
                    let database_name = table.get_database_name();
                    let table_name = table.get_table_name();
                    let columns = table.get_column_infos()
                        .iter()
                        .map(|c| {
                            RelayColumnInfo::from_binlog_column_info(c)
                        }).collect();
                    let relay_command = RelayCommand::Insert(insert_rows);
                    Self {
                        src_type,
                        event_log_pos,
                        event_name,
                        database_name,
                        table_name,
                        columns,
                        relay_command,
                    }
                } else {
                    Self::default()
                }
            }
            BinlogEvent::UpdateRows(e) => {
                if let Some(table) = e.get_table_map_event() {
                    let event_log_pos = e.get_header().get_log_pos();
                    let event_name = e.get_type_name();
                    let update_rows: Vec<(RelayRowData, RelayRowData)> = e.rows
                        .iter()
                        .map(|r| {
                            (RelayRowData::from_binlog_row(&(r.get_before_update())), RelayRowData::from_binlog_row(&(r.get_after_update())))
                        }).collect();
                    let database_name = table.get_database_name();
                    let table_name = table.get_table_name();
                    let columns = table.get_column_infos()
                        .iter()
                        .map(|c| {
                            RelayColumnInfo::from_binlog_column_info(c)
                        }).collect();
                    let relay_command = RelayCommand::Update(update_rows);
                    Self {
                        src_type,
                        event_log_pos,
                        event_name,
                        database_name,
                        table_name,
                        columns,
                        relay_command,
                    }
                } else {
                    Self::default()
                }
            }
            BinlogEvent::DeleteRows(e) => {
                if let Some(table) = e.get_table_map_event() {
                    let event_log_pos = e.get_header().get_log_pos();
                    let event_name = e.get_type_name();
                    let delete_rows: Vec<RelayRowData> = e.get_rows().iter().map(|r| {
                        RelayRowData::from_binlog_row(r)
                    }).collect();
                    let database_name = table.get_database_name();
                    let table_name = table.get_table_name();
                    let columns = table.get_column_infos()
                        .iter()
                        .map(|c| {
                            RelayColumnInfo::from_binlog_column_info(c)
                        }).collect();
                    let relay_command = RelayCommand::Delete(delete_rows);
                    Self {
                        src_type,
                        event_log_pos,
                        event_name,
                        database_name,
                        table_name,
                        columns,
                        relay_command,
                    }
                } else {
                    Self::default()
                }
            }
            _ => {
                // todo 其它event后续实现
                Self::default()
            }
        }
    }

    pub fn get_database_name(&self) -> &str {
        self.database_name.as_str()
    }

    pub fn get_table_name(&self) -> &str {
        self.table_name.as_str()
    }
}

impl Default for RelayLog {
    fn default() -> Self {
        Self {
            src_type: SrcType::default(),
            event_log_pos: 0,
            event_name: "".to_string(),
            database_name: "".to_string(),
            table_name: "".to_string(),
            columns: vec![],
            relay_command: RelayCommand::None,
        }
    }
}