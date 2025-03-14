use crate::events::event_header::Header;
use crate::events::log_context::{ILogContext, LogContext, LogContextRef};
use crate::events::declare::log_event::{LogEvent, QUERY_HEADER_LEN, QUERY_HEADER_MINIMAL_LEN};
use crate::events::query;
use crate::utils::{extract_string};
use crate::QueryStatusVar;
use common::err::decode_error::ReError;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Buf;
use sqlparser::ast::{AlterTableOperation, ColumnDef, Statement};
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::parser::{Parser, ParserError};
use common::binlog::column::column_type::SrcColumnType;
use common::err::CResult;
use crate::ast::query_parser::{QueryParser, TableInfo, TableInfoBuilder};
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::ext::decode_error_ext::decode_error_from;
use crate::events::event_raw::HeaderRef;
use crate::events::protocol::table_map_event::TableMapEvent;

/// The maximum number of updated databases that a status of Query-log-event
/// can carry. It can redefined within a range [1..
/// OVER_MAX_DBS_IN_EVENT_MTS].
pub const MAX_DBS_IN_EVENT_MTS: u32 = 16;

/// When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS the
/// value of OVER_MAX_DBS_IN_EVENT_MTS is is put into the mts_accessed_dbs
/// status.
pub const OVER_MAX_DBS_IN_EVENT_MTS: u32 = 254;

pub const SYSTEM_CHARSET_MBMAXLEN: u8 = 3;
pub const NAME_CHAR_LEN: u8 = 64;
///Field/table name length
pub const NAME_LEN: u8 = (NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN);

/// Max number of possible extra bytes in a replication event compared to a
/// packet (i.e. a query) sent from client to master; First, an auxiliary
/// log_event status vars estimation:
pub const MAX_SIZE_LOG_EVENT_STATUS:u32 = (1 + 4 /* type, flags2 */
                                                         + 1 + 8 /*
                                                                  * type,
                                                                  * sql_mode
                                                                  */
                                                         + 1 + 1 + 255/*
                                                                       * type,
                                                                       * length
                                                                       * ,
                                                                       * catalog
                                                                       */
                                                         + 1 + 4 /*
                                                                  * type,
                                                                  * auto_increment
                                                                  */
                                                         + 1 + 6 /*
                                                                  * type,
                                                                  * charset
                                                                  */
                                                         + 1 + 1 + 255 /*
                                                                        * type,
                                                                        * length
                                                                        * ,
                                                                        * time_zone
                                                                        */
                                                         + 1 + 2 /*
                                                                  * type,
                                                                  * lc_time_names_number
                                                                  */
                                                         + 1 + 2 /*
                                                                  * type,
                                                                  * charset_database_number
                                                                  */
                                                         + 1 + 8 /*
                                                                  * type,
                                                                  * table_map_for_update
                                                                  */
                                                         + 1 + 4 /*
                                                                  * type,
                                                                  * master_data_written
                                                                  */
                                                        /*
                                                         * type, db_1, db_2,
                                                         * ...
                                                         */
                                                        /* type, microseconds */
                                                        /*
                                                         * MariaDb type,
                                                         * sec_part of NOW()
                                                         */
                                                        + 1 + (MAX_DBS_IN_EVENT_MTS * (1 + NAME_LEN as u32)) + 3 /*
                                                                                                            * type
                                                                               `                             * ,
                                                                                                            * microseconds
                                                                                                            */+ 1 + 32
                                                         * 3 + 1 + 60/*
                                                                      * type ,
                                                                      * user_len
                                                                      * , user ,
                                                                      * host_len
                                                                      * , host
                                                                      */)
                                                        + 1 + 1 /*
                                                                 * type,
                                                                 * explicit_def
                                                                 * ..ts
                                                                 */+ 1 + 8 /*
                                                                            * type,
                                                                            * xid
                                                                            * of
                                                                            * DDL
                                                                            */+ 1 + 2 /*
                                                                                       * type
                                                                                       * ,
                                                                                       * default_collation_for_utf8mb4_number
                                                                                       */+ 1 /* sql_require_primary_key */
;

/// query event post-header
pub const Q_THREAD_ID_OFFSET: u8 = 0;
pub const Q_EXEC_TIME_OFFSET: u8 = 4;
pub const Q_DB_LEN_OFFSET: u8 = 8;
pub const Q_ERR_CODE_OFFSET: u8 = 9;
pub const Q_STATUS_VARS_LEN_OFFSE: u8 = 11;
pub const Q_DATA_OFFSET: u8 = QUERY_HEADER_LEN;

/// 记录更新操作的语句
///
/// A Query_log_event is created for each query that modifies the database,
/// unless the query is logged row-based. The Post-Header has five components:
/// <table>
/// <caption>Post-Header for Query_log_event</caption>
/// <tr>
/// <th>Name</th>
/// <th>Format</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>slave_proxy_id</td>
/// <td>4 byte unsigned integer</td>
/// <td>An integer identifying the client thread that issued the query. The id is
/// unique per server. (Note, however, that two threads on different servers may
/// have the same slave_proxy_id.) This is used when a client thread creates a
/// temporary table local to the client. The slave_proxy_id is used to
/// distinguish temporary tables that belong to different clients.</td>
/// </tr>
/// <tr>
/// <td>exec_time</td>
/// <td>4 byte unsigned integer</td>
/// <td>The time from when the query started to when it was logged in the binlog,
/// in seconds.</td>
/// </tr>
/// <tr>
/// <td>db_len</td>
/// <td>1 byte integer</td>
/// <td>The length of the name of the currently selected database.</td>
/// </tr>
/// <tr>
/// <td>error_code</td>
/// <td>2 byte unsigned integer</td>
/// <td>Error code generated by the master. If the master fails, the slave will
/// fail with the same error code, except for the error codes ER_DB_CREATE_EXISTS
/// == 1007 and ER_DB_DROP_EXISTS == 1008.</td>
/// </tr>
/// <tr>
/// <td>status_vars_len</td>
/// <td>2 byte unsigned integer</td>
/// <td>The length of the status_vars block of the Body, in bytes. See
/// query_log_event_status_vars "below".</td>
/// </tr>
/// </table>
/// The Body has the following components:
/// <table>
/// <caption>Body for Query_log_event</caption>
/// <tr>
/// <th>Name</th>
/// <th>Format</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>query_log_event_status_vars status_vars</td>
/// <td>status_vars_len bytes</td>
/// <td>Zero or more status variables. Each status variable consists of one byte
/// identifying the variable stored, followed by the value of the variable. The
/// possible variables are listed separately in the table
/// Table_query_log_event_status_vars "below". MySQL always writes events in the
/// order defined below; however, it is capable of reading them in any order.</td>
/// </tr>
/// <tr>
/// <td>db</td>
/// <td>db_len+1</td>
/// <td>The currently selected database, as a null-terminated string. (The
/// trailing zero is redundant since the length is already known; it is db_len
/// from Post-Header.)</td>
/// </tr>
/// <tr>
/// <td>query</td>
/// <td>variable length string without trailing zero, extending to the end of the
/// event (determined by the length field of the Common-Header)</td>
/// <td>The SQL query.</td>
/// </tr>
/// </table>
/// The following table lists the status variables that may appear in the
/// status_vars field. Table_query_log_event_status_vars
/// <table>
/// <caption>Status variables for Query_log_event</caption>
/// <tr>
/// <th>Status variable</th>
/// <th>1 byte identifier</th>
/// <th>Format</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>flags2</td>
/// <td>Q_FLAGS2_CODE == 0</td>
/// <td>4 byte bitfield</td>
/// <td>The flags in thd->options, binary AND-ed with OPTIONS_WRITTEN_TO_BIN_LOG.
/// The thd->options bitfield contains options for "SELECT". OPTIONS_WRITTEN
/// identifies those options that need to be written to the binlog (not all do).
/// Specifically, OPTIONS_WRITTEN_TO_BIN_LOG equals (OPTION_AUTO_IS_NULL |
/// OPTION_NO_FOREIGN_KEY_CHECKS | OPTION_RELAXED_UNIQUE_CHECKS |
/// OPTION_NOT_AUTOCOMMIT), or 0x0c084000 in hex. These flags correspond to the
/// SQL variables SQL_AUTO_IS_NULL, FOREIGN_KEY_CHECKS, UNIQUE_CHECKS, and
/// AUTOCOMMIT, documented in the "SET Syntax" section of the MySQL Manual. This
/// field is always written to the binlog in version >= 5.0, and never written in
/// version < 5.0.</td>
/// </tr>
/// <tr>
/// <td>sql_mode</td>
/// <td>Q_SQL_MODE_CODE == 1</td>
/// <td>8 byte bitfield</td>
/// <td>The sql_mode variable. See the section "SQL Modes" in the MySQL manual,
/// and see mysql_priv.h for a list of the possible flags. Currently
/// (2007-10-04), the following flags are available:
///
/// <pre>
///     MODE_REAL_AS_FLOAT==0x1
///     MODE_PIPES_AS_CONCAT==0x2
///     MODE_ANSI_QUOTES==0x4
///     MODE_IGNORE_SPACE==0x8
///     MODE_NOT_USED==0x10
///     MODE_ONLY_FULL_GROUP_BY==0x20
///     MODE_NO_UNSIGNED_SUBTRACTION==0x40
///     MODE_NO_DIR_IN_CREATE==0x80
///     MODE_POSTGRESQL==0x100
///     MODE_ORACLE==0x200
///     MODE_MSSQL==0x400
///     MODE_DB2==0x800
///     MODE_MAXDB==0x1000
///     MODE_NO_KEY_OPTIONS==0x2000
///     MODE_NO_TABLE_OPTIONS==0x4000
///     MODE_NO_FIELD_OPTIONS==0x8000
///     MODE_MYSQL323==0x10000
///     MODE_MYSQL323==0x20000
///     MODE_MYSQL40==0x40000
///     MODE_ANSI==0x80000
///     MODE_NO_AUTO_VALUE_ON_ZERO==0x100000
///     MODE_NO_BACKSLASH_ESCAPES==0x200000
///     MODE_STRICT_TRANS_TABLES==0x400000
///     MODE_STRICT_ALL_TABLES==0x800000
///     MODE_NO_ZERO_IN_DATE==0x1000000
///     MODE_NO_ZERO_DATE==0x2000000
///     MODE_INVALID_DATES==0x4000000
///     MODE_ERROR_FOR_DIVISION_BY_ZERO==0x8000000
///     MODE_TRADITIONAL==0x10000000
///     MODE_NO_AUTO_CREATE_USER==0x20000000
///     MODE_HIGH_NOT_PRECEDENCE==0x40000000
///     MODE_PAD_CHAR_TO_FULL_LENGTH==0x80000000
/// </pre>
///
/// All these flags are replicated from the server. However, all flags except
/// MODE_NO_DIR_IN_CREATE are honored by the slave; the slave always preserves
/// its old value of MODE_NO_DIR_IN_CREATE. For a rationale, see comment in
/// Query_log_event::do_apply_event in log_event.cc. This field is always written
/// to the binlog.</td>
/// </tr>
/// <tr>
/// <td>catalog</td>
/// <td>Q_CATALOG_NZ_CODE == 6</td>
/// <td>Variable-length string: the length in bytes (1 byte) followed by the
/// characters (at most 255 bytes)</td>
/// <td>Stores the client's current catalog. Every database belongs to a catalog,
/// the same way that every table belongs to a database. Currently, there is only
/// one catalog, "std". This field is written if the length of the catalog is >
/// 0; otherwise it is not written.</td>
/// </tr>
/// <tr>
/// <td>auto_increment</td>
/// <td>Q_AUTO_INCREMENT == 3</td>
/// <td>two 2 byte unsigned integers, totally 2+2=4 bytes</td>
/// <td>The two variables auto_increment_increment and auto_increment_offset, in
/// that order. For more information, see "System variables" in the MySQL manual.
/// This field is written if auto_increment > 1. Otherwise, it is not written.</td>
/// </tr>
/// <tr>
/// <td>charset</td>
/// <td>Q_CHARSET_CODE == 4</td>
/// <td>three 2 byte unsigned integers, totally 2+2+2=6 bytes</td>
/// <td>The three variables character_set_client, collation_connection, and
/// collation_server, in that order. character_set_client is a code identifying
/// the character set and collation used by the client to encode the query.
/// collation_connection identifies the character set and collation that the
/// master converts the query to when it receives it; this is useful when
/// comparing literal strings. collation_server is the default character set and
/// collation used when a new database is created. See also
/// "Connection Character Sets and Collations" in the MySQL 5.1 manual. All three
/// variables are codes identifying a (character set, collation) pair. To see
/// which codes map to which pairs, run the query "SELECT id, character_set_name,
/// collation_name FROM COLLATIONS". Cf. Q_CHARSET_DATABASE_CODE below. This
/// field is always written.</td>
/// </tr>
/// <tr>
/// <td>time_zone</td>
/// <td>Q_TIME_ZONE_CODE == 5</td>
/// <td>Variable-length string: the length in bytes (1 byte) followed by the
/// characters (at most 255 bytes).
/// <td>The time_zone of the master. See also "System Variables" and
/// "MySQL Server Time Zone Support" in the MySQL manual. This field is written
/// if the length of the time zone string is > 0; otherwise, it is not written.</td>
/// </tr>
/// <tr>
/// <td>lc_time_names_number</td>
/// <td>Q_LC_TIME_NAMES_CODE == 7</td>
/// <td>2 byte integer</td>
/// <td>A code identifying a table of month and day names. The mapping from codes
/// to languages is defined in sql_locale.cc. This field is written if it is not
/// 0, i.e., if the locale is not en_US.</td>
/// </tr>
/// <tr>
/// <td>charset_database_number</td>
/// <td>Q_CHARSET_DATABASE_CODE == 8</td>
/// <td>2 byte integer</td>
/// <td>The value of the collation_database system variable (in the source code
/// stored in thd->variables.collation_database), which holds the code for a
/// (character set, collation) pair as described above (see Q_CHARSET_CODE).
/// collation_database was used in old versions (???WHEN). Its value was loaded
/// when issuing a "use db" query and could be changed by issuing a
/// "SET collation_database=xxx" query. It used to affect the "LOAD DATA INFILE"
/// and "CREATE TABLE" commands. In newer versions, "CREATE TABLE" has been
/// changed to take the character set from the database of the created table,
/// rather than the character set of the current database. This makes a
/// difference when creating a table in another database than the current one.
/// "LOAD DATA INFILE" has not yet changed to do this, but there are plans to
/// eventually do it, and to make collation_database read-only. This field is
/// written if it is not 0.</td>
/// </tr>
/// <tr>
/// <td>table_map_for_update</td>
/// <td>Q_TABLE_MAP_FOR_UPDATE_CODE == 9</td>
/// <td>8 byte integer</td>
/// <td>The value of the table map that is to be updated by the multi-table
/// update query statement. Every bit of this variable represents a table, and is
/// set to 1 if the corresponding table is to be updated by this statement. The
/// value of this variable is set when executing a multi-table update statement
/// and used by slave to apply filter rules without opening all the tables on
/// slave. This is required because some tables may not exist on slave because of
/// the filter rules.</td>
/// </tr>
/// </table>
///
///
/// Query_log_event_notes_on_previous_versions Notes on Previous Versions Status
/// vars were introduced in version 5.0. To read earlier versions correctly,
/// check the length of the Post-Header. The status variable Q_CATALOG_CODE == 2
/// existed in MySQL 5.0.x, where 0<=x<=3. It was identical to Q_CATALOG_CODE,
/// except that the string had a trailing '\0'. The '\0' was removed in 5.0.4
/// since it was redundant (the string length is stored before the string). The
/// Q_CATALOG_CODE will never be written by a new master, but can still be
/// understood by a new slave. See Q_CHARSET_DATABASE_CODE in the table above.
/// When adding new status vars, please don't forget to update the
/// MAX_SIZE_LOG_EVENT_STATUS, and update function code_name
///
/// @see mysql-5.1.6/sql/logevent.cc - Query_log_event
///
/// doc: https://dev.mysql.com/doc/internals/en/query-event.html
/// source: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/statement_events.h#L44-L426
/// layout: https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/statement_events.h#L627-L643
#[derive(Debug, Serialize, Clone)]
pub struct QueryEvent {
    header: Header,

    ////////////////////////////////////////////////////
    //   post-header部分
    ////////////////////////////////////////////////////
    /// thread_id, 小端存储，执行语句的线程ID号。
    /// 可以用于审计
    pub thread_id: u32,

    /// 小端存储，语句执行的时间，单位秒
    execution_time: u32,

    /// 执行命令时候所在的database名的字节长度, length of current select schema name
    pub schema_length: u8,

    /// 	错误号
    error_code: u16,

    /// 小端存储，这部分，在v1和v3版本的event中是没有的，在v4版本存在，记录status_vars的长度的长度
    status_vars_length: u16,

    ////////////////////////////////////////////////////
    //   event-body部分
    ////////////////////////////////////////////////////
    /// 记录状态值。 字节数为： status_vars_len字节
    status_vars: Vec<QueryStatusVar>,

    /// null-terminaled类型的字符串，记录database的名字。 字节数 db_len+1字节
    pub schema: String,

    /// 执行的语句。 长度不确定
    pub query: String,

    /// 	校验码。 4个字节
    checksum: u32,

    //////// ext
    table_info: Option<TableInfo>,
}

impl QueryEvent {
    pub fn parse_with_compress<'a>(
        cursor: &mut Cursor<&[u8]>,
        mut header: HeaderRef,
        compatiable_percona: bool,
        compress: bool,
        shard_context: LogContextRef,
    ) -> Result<QueryEvent, ReError> {
        let context = shard_context.borrow_mut();

        let common_header_len = context.get_format_description().common_header_len;
        let query_post_header_len = context
            .get_format_description()
            .get_post_header_len(header.borrow().get_event_type() as usize);

        // event-body 部分长度
        let mut data_len =
            header.borrow().get_event_length() - (common_header_len + query_post_header_len) as u32;

        // Q_THREAD_ID_OFFSET
        let thread_id = cursor.read_u32::<LittleEndian>()?;
        // Q_EXEC_TIME_OFFSET
        let execution_time = cursor.read_u32::<LittleEndian>()?;
        // Q_DB_LEN_OFFSET
        let schema_length = cursor.read_u8()?;
        // Q_ERR_CODE_OFFSET
        let error_code = cursor.read_u16::<LittleEndian>()?;

        // 5.0 format starts here. Depending on the format, we may or not have
        // affected/warnings etc The remaining post-header to be parsed has length:
        let status_vars_len = if query_post_header_len > QUERY_HEADER_MINIMAL_LEN {
            // Q_STATUS_VARS_LEN_OFFSET
            let status_vars_len = cursor.read_u16::<LittleEndian>()?;

            /*
             * Check if status variable length is corrupt and will lead to very
             * wrong data. We could be even more strict and require data_len to
             * be even bigger, but this will suffice to catch most corruption
             * errors that can lead to a crash.
             */
            let min = if data_len > MAX_SIZE_LOG_EVENT_STATUS {
                MAX_SIZE_LOG_EVENT_STATUS
            } else {
                data_len
            } as u16;
            if status_vars_len > min {
                let err = ReError::String("status_vars_length (".to_owned() + (status_vars_len as u16).to_string().as_str() + ") \
                            > data_len (" + (data_len as u16).to_string().as_str() + ")");

                return Err(err);
            }

            status_vars_len
        } else {
            0
        };

        // 计算真正的 Variable data部分长度
        data_len -= status_vars_len as u32;

        /* variable-part: the status vars; only in MySQL 5.0 */
        let mut _status_vars = vec![0; status_vars_len as usize];
        cursor.read_exact(&mut _status_vars)?;
        let mut _raw_vars_cursor = Cursor::new(_status_vars.as_slice());
        let status_vars =
            QueryEvent::unpack_variables(&mut _raw_vars_cursor, compatiable_percona)?;
        assert!(!_raw_vars_cursor.has_remaining());

        /* A 2nd variable part; this is common to all versions */
        let mut _db_name = vec![0; (schema_length + 1) as usize];
        cursor.read_exact(&mut _db_name)?;
        let schema = String::from_utf8(_db_name[0..schema_length as usize].to_vec()).unwrap();

        let query_len =
            // header.get_event_length()                       //--
            // - common_header_len as u32                      // --
            // - query_post_header_len as u32                  // --  is  data_len
            // - status_vars_len as u32                        // --
            data_len
            - schema_length as u32
            - 1
            - 4 /* checksum size */
            ;
        let mut _query = vec![0; query_len as usize];
        cursor.read_exact(&mut _query)?;
        let query = extract_string(&_query);

        let table_info = match QueryParser::new(query.clone()).parser_ddl_table_format() {
            Ok(t) => {t}
            Err(_) => {None}
        };

        let checksum = cursor.read_u32::<LittleEndian>()?;

        Ok(QueryEvent {
                header: Header::copy_and_get(header, checksum, HashMap::new()),
                thread_id,
                execution_time,
                schema_length,
                error_code,
                status_vars_length: status_vars_len,
                status_vars,
                schema,
                query,
                checksum,
                table_info
        })
    }

    fn unpack_variables(
        raw_vars: &mut Cursor<&[u8]>,
        compatiable_percona: bool) -> Result<Vec<QueryStatusVar>, ReError> {
        let mut rs = Vec::<QueryStatusVar>::new();
        while raw_vars.has_remaining() {
            let it = query::parse_status_var_cursor(raw_vars);

            rs.push(it.unwrap());
        }

        Ok(rs)
    }

    pub fn get_table_info(&self) -> Option<&TableInfo> {
        self.table_info.as_ref()
    }

    pub fn has_table_info(&self) -> bool {
        self.table_info.is_some()
    }
}

impl LogEvent for QueryEvent {
    fn get_type_name(&self) -> String {
        "QueryEvent".to_string()
    }

    fn len(&self) -> i32 {
        self.header.get_event_length() as i32
    }

    fn parse(
        cursor: &mut Cursor<&[u8]>,
        mut header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<QueryEvent, ReError> {
        QueryEvent::parse_with_compress(cursor, header, false, false, context)
    }
}
