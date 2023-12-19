use nom::{
    bytes::complete::take,
    combinator::map,
    multi::many_m_n,
    number::complete::{le_u16, le_u32, le_u64, le_u8},
    sequence::tuple, IResult, Parser
};
use nom::number::complete::le_u24;
use serde::Serialize;
use crate::events::protocol::query_event::{MAX_DBS_IN_EVENT_MTS, OVER_MAX_DBS_IN_EVENT_MTS};

use crate::utils::{string_by_variable_len, extract_string, pu32, string_by_nul_terminated};

/// 状态的类型. not more than 256 values (1 byte).
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum QueryStatusVar {
    /// 对应的code	状态值占用的字节数
    /// 0	4字节
    Q_FLAGS2_CODE(Q_FLAGS2_CODE_VAL),
    /// 1	8字节
    Q_SQL_MODE_CODE(Q_SQL_MODE_CODE_VAL),
    /// 2	第一个字节表示catalog_len，总共catalog_len+2个字节
    /// Q_CATALOG_CODE is catalog with end zero stored; it is used only by MySQL
    /// 5.0.x where 0<=x<=3. We have to keep it to be able to replicate these old masters.
    Q_CATALOG(String),
    /// 3	4字节
    Q_AUTO_INCREMENT(u16, u16),
    /// 4	6字节
    Q_CHARSET_CODE(u16, u16, u16),
    /// 5	第一个字节表示time_zone_len，总共time_zone_len+1字节
    Q_TIME_ZONE_CODE(String),
    /// code is 6.
    /// 第一个字节表示catalog_len，总共catalog_len+1个字节.
    ///
    /// Q_CATALOG_NZ_CODE is catalog withOUT end zero stored; it is used by MySQL
    /// 5.0.x where x>=4. Saves one byte in every Query_log_event in binlog,
    /// compared to Q_CATALOG_CODE. The reason we didn't simply re-use
    /// Q_CATALOG_CODE is that then a 5.0.3 slave of this 5.0.x (x>=4) master
    /// would crash (segfault etc) because it would expect a 0 when there is none.
    Q_CATALOG_NZ_CODE(String),
    /// 7	2字节
    Q_LC_TIME_NAMES_CODE(u16),
    /// 8	2字节
    Q_CHARSET_DATABASE_CODE(u16),
    /// 9	8字节
    Q_TABLE_MAP_FOR_UPDATE_CODE(u64),
    /// 10	4字节
    Q_MASTER_DATA_WRITTEN_CODE(u32),
    /// 11	包含两部分，一部分是user，一部分是host。
    /// user部分，一个字节表示user_len，接着user_len个字节表示user。
    /// host部分，一个字节表示host_len，接着host_len个字节表示host。
    Q_INVOKERS(String, String),
    /// 12
    /// Q_UPDATED_DB_NAMES status variable collects of the updated databases
    /// total number and their names to be propagated to the slave in order to
    /// facilitate the parallel applying of the Query events.
    Q_UPDATED_DB_NAMES(Vec<String>),
    /// 13	3字节, this field take 3 bytes
    Q_MICROSECONDS(u32),
    /// code is 14
    /// A old (unused now) code for Query_log_event status similar to G_COMMIT_TS.
    Q_COMMIT_TS,
    /// code is 15,  A code for Query_log_event status, similar to G_COMMIT_TS2.
    Q_COMMIT_TS2,
    /// code is 16,	1 byte,
    /// The master connection @@session.explicit_defaults_for_timestamp which is
    /// recorded for queries, CREATE and ALTER table that is defined with a
    /// TIMESTAMP column, that are dependent on that feature. For pre-WL6292
    /// master's the associated with this code value is zero.
    Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP(u8),

    /// code is 17.
    /// The variable carries xid info of 2pc-aware (recoverable) DDL queries.
    Q_DDL_LOGGED_WITH_XID(u64),

    /// code is 18,
    /// This variable stores the default collation for the utf8mb4 character set.
    /// Used to support cross-version replication.
    Q_DEFAULT_COLLATION_FOR_UTF8MB4(u16),

    /// code is 19, Replicate sql_require_primary_key.
    Q_SQL_REQUIRE_PRIMARY_KEY(u8),

    /// code is 20, Replicate default_table_encryption.
    Q_DEFAULT_TABLE_ENCRYPTION(u8),

    /// code is 21, percona 8.0.31 Replicate ddl_skip_rewrite.
    Q_DDL_SKIP_REWRITE(u8),

    /// code is 128, percona 8.0.31 Replicate Q_WSREP_SKIP_READONLY_CHECKS.
    Q_WSREP_SKIP_READONLY_CHECKS,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Q_FLAGS2_CODE_VAL {
    pub auto_is_null: bool,
    pub auto_commit: bool,
    pub foreign_key_checks: bool,
    pub unique_checks: bool,
}

impl From<u32> for Q_FLAGS2_CODE_VAL {
    fn from(value: u32) -> Self {
        let auto_is_null = (value >> 14) % 2 == 1;
        let auto_commit = (value >> 19) % 2 == 0;
        let foreign_key_checks = (value >> 26) % 2 == 0;
        let unique_checks = (value >> 27) % 2 == 0;

        Q_FLAGS2_CODE_VAL {
            auto_is_null,
            auto_commit,
            foreign_key_checks,
            unique_checks,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct Q_SQL_MODE_CODE_VAL {
    pub real_as_float: bool,
    pub pipes_as_concat: bool,
    pub ansi_quotes: bool,
    pub ignore_space: bool,
    pub not_used: bool,
    pub only_full_group_by: bool,
    pub no_unsigned_subtraction: bool,
    pub no_dir_in_create: bool,
    pub postgresql: bool,
    pub oracle: bool,
    pub mssql: bool,
    pub db2: bool,
    pub maxdb: bool,
    pub no_key_options: bool,
    pub no_table_options: bool,
    pub no_field_options: bool,
    pub mysql323: bool,
    pub mysql40: bool,
    pub ansi: bool,
    pub no_auto_value_on_zero: bool,
    pub no_backslash_escapes: bool,
    pub strict_trans_tables: bool,
    pub strict_all_tables: bool,
    pub no_zero_in_date: bool,
    pub no_zero_date: bool,
    pub invalid_dates: bool,
    pub error_for_division_by_zero: bool,
    pub traditional: bool,
    pub no_auto_create_user: bool,
    pub high_not_precedence: bool,
    pub no_engine_substitution: bool,
    pub pad_char_to_full_length: bool,
}

impl From<u64> for Q_SQL_MODE_CODE_VAL {
    fn from(value: u64) -> Self {
        Q_SQL_MODE_CODE_VAL {
            real_as_float: (value >> 0) % 2 == 1,
            pipes_as_concat: (value >> 1) % 2 == 1,
            ansi_quotes: (value >> 2) % 2 == 1,
            ignore_space: (value >> 3) % 2 == 1,
            not_used: (value >> 4) % 2 == 1,
            only_full_group_by: (value >> 5) % 2 == 1,
            no_unsigned_subtraction: (value >> 6) % 2 == 1,
            no_dir_in_create: (value >> 7) % 2 == 1,
            postgresql: (value >> 8) % 2 == 1,
            oracle: (value >> 9) % 2 == 1,
            mssql: (value >> 10) % 2 == 1,
            db2: (value >> 11) % 2 == 1,
            maxdb: (value >> 12) % 2 == 1,
            no_key_options: (value >> 13) % 2 == 1,
            no_table_options: (value >> 14) % 2 == 1,
            no_field_options: (value >> 15) % 2 == 1,
            mysql323: (value >> 16) % 2 == 1,
            mysql40: (value >> 17) % 2 == 1,
            ansi: (value >> 18) % 2 == 1,
            no_auto_value_on_zero: (value >> 19) % 2 == 1,
            no_backslash_escapes: (value >> 20) % 2 == 1,
            strict_trans_tables: (value >> 21) % 2 == 1,
            strict_all_tables: (value >> 22) % 2 == 1,
            no_zero_in_date: (value >> 23) % 2 == 1,
            no_zero_date: (value >> 24) % 2 == 1,
            invalid_dates: (value >> 25) % 2 == 1,
            error_for_division_by_zero: (value >> 26) % 2 == 1,
            traditional: (value >> 27) % 2 == 1,
            no_auto_create_user: (value >> 28) % 2 == 1,
            high_not_precedence: (value >> 29) % 2 == 1,
            no_engine_substitution: (value >> 30) % 2 == 1,
            pad_char_to_full_length: (value >> 31) % 2 == 1,
        }
    }
}
//
// impl Into<u32> for QueryStatusVar {
//     fn into(self) -> u32 {
//         match self {
//             Q_FLAGS2_CODE => 0,
//             Q_SQL_MODE_CODE => 1,
//             Q_CATALOG => 2,
//             Q_AUTO_INCREMENT => 3,
//             Q_CHARSET_CODE => 4,
//             Q_TIME_ZONE_CODE => 5,
//             Q_CATALOG_NZ_CODE => 6,
//             Q_LC_TIME_NAMES_CODE => 7,
//             Q_CHARSET_DATABASE_CODE => 8,
//             Q_TABLE_MAP_FOR_UPDATE_CODE => 9,
//             Q_MASTER_DATA_WRITTEN_CODE => 10,
//             Q_INVOKERS => 11,
//             Q_UPDATED_DB_NAMES => 12,
//             Q_MICROSECONDS => 13,
//             Q_COMMIT_TS => 14,
//             Q_COMMIT_TS2 => 15,
//             Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP => 16,
//             Q_DDL_LOGGED_WITH_XID => 17,
//             Q_DEFAULT_COLLATION_FOR_UTF8MB4 => 18,
//             Q_SQL_REQUIRE_PRIMARY_KEY => 19,
//             Q_DEFAULT_TABLE_ENCRYPTION => 20,
//             Q_DDL_SKIP_REWRITE => 21,
//             Q_WSREP_SKIP_READONLY_CHECKS => 128,
//         }
//     }
// }

pub fn parse_status_var<'a>(input: &'a [u8]) -> IResult<&'a [u8], QueryStatusVar> {
    let (i, code) = le_u8(input)?;

    match code {
        0x00 => { // Q_FLAGS2_CODE
            let (i, code) = le_u32(i)?;
            Ok((i, QueryStatusVar::Q_FLAGS2_CODE(Q_FLAGS2_CODE_VAL::from(code))))
        }
        0x01 => { // Q_SQL_MODE_CODE
            let (i, code) = le_u64(i)?; // when sql_mode is ulonglong
            Ok((i, QueryStatusVar::Q_SQL_MODE_CODE(Q_SQL_MODE_CODE_VAL::from(code))))
        }
        0x02 => { //Q_CATALOG, for 5.0.x where 0<=x<=3 masters
            let (i, len) = le_u8(i)?;
            let (i, val) = map(take(len), |s: &[u8]| string_by_variable_len(s, len as usize))(i)?;
            let (i, term) = le_u8(i)?;
            assert_eq!(term, 0x00);
            Ok((i, QueryStatusVar::Q_CATALOG(val)))
        }
        0x03 => { // Q_AUTO_INCREMENT
            let (i, incr) = le_u16(i)?;
            let (i, offset) = le_u16(i)?;
            Ok((i, QueryStatusVar::Q_AUTO_INCREMENT(incr, offset)))
        }
        0x04 => { // Q_CHARSET_CODE
            // Charset: 6 byte character set flag.
            // 1-2 = character set client
            // 3-4 = collation client
            // 5-6 = collation server
            let (i, (client, conn, server)) = tuple((le_u16, le_u16, le_u16))(i)?;
            Ok((i, QueryStatusVar::Q_CHARSET_CODE(client, conn, server)))
        }
        0x05 => { // Q_TIME_ZONE_CODE
            let (i, len) = le_u8(i)?;
            let (i, tz) = map(take(len), |s: &[u8]| extract_string(s))(i)?;
            Ok((i, QueryStatusVar::Q_TIME_ZONE_CODE(tz)))
        }
        0x06 => { // Q_CATALOG_NZ_CODE
            let (i, len) = le_u8(i)?;
            let (i, val) = map(take(len), |s: &[u8]| extract_string(s))(i)?;
            Ok((i, QueryStatusVar::Q_CATALOG_NZ_CODE(val)))
        }
        0x07 => { // Q_LC_TIME_NAMES_CODE
            map(le_u16, |v| QueryStatusVar::Q_LC_TIME_NAMES_CODE(v))(i)
        },
        0x08 => { // Q_CHARSET_DATABASE_CODE
            map(le_u16, |v| QueryStatusVar::Q_CHARSET_DATABASE_CODE(v))(i)
        },
        0x09 => { // Q_TABLE_MAP_FOR_UPDATE_CODE => 9
            map(le_u64, |v| QueryStatusVar::Q_TABLE_MAP_FOR_UPDATE_CODE(v))(i)
        },
        0x0a => { // Q_MASTER_DATA_WRITTEN_CODE => 10
            map(le_u32, |v| QueryStatusVar::Q_MASTER_DATA_WRITTEN_CODE(v))(i)
        },
        0x0b => { // Q_INVOKERS => 11
            let (i, len) = le_u8(i)?;
            let (i, user) = map(take(len), |s: &[u8]| string_by_variable_len(s, len as usize))(i)?;

            let (i, len) = le_u8(i)?;
            let (i, host) = map(take(len), |s: &[u8]| string_by_variable_len(s, len as usize))(i)?;
            Ok((i, QueryStatusVar::Q_INVOKERS(user, host)))
        }
        0x0c => { // Q_UPDATED_DB_NAMES => 12
            let (i, mut mts_accessed_dbs) = le_u8(i)?;

            /**
             * Notice, the following check is positive also in case
             * of the master's MAX_DBS_IN_EVENT_MTS > the slave's
             * one and the event contains e.g the master's MAX_DBS_IN_EVENT_MTS db:s.
             */
            if mts_accessed_dbs > MAX_DBS_IN_EVENT_MTS as u8 {
                mts_accessed_dbs = OVER_MAX_DBS_IN_EVENT_MTS as u8;
                return Ok((i, QueryStatusVar::Q_UPDATED_DB_NAMES(Vec::with_capacity(0))));
            }

            let (i, mts_accessed_db_names) =
                many_m_n(mts_accessed_dbs as usize, mts_accessed_dbs as usize, string_by_nul_terminated)(i)?;
            Ok((i, QueryStatusVar::Q_UPDATED_DB_NAMES(mts_accessed_db_names)))
        }
        0x0d => { // Q_MICROSECONDS => 13
            // map(pu32, |val| QueryStatusVar::Q_MICROSECONDS(val))(i)
            map(le_u24, |val| QueryStatusVar::Q_MICROSECONDS(val))(i)
        },
        // Q_COMMIT_TS => 14, Q_COMMIT_TS2 => 15 unused now
        0x10 => { // Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP => 16
            // thd->variables.explicit_defaults_for_timestamp
            let (i, explicit_defaults_for_timestamp) = le_u8(i)?;
            Ok((i, QueryStatusVar::Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP(explicit_defaults_for_timestamp)))
        },
        0x11 => { // Q_DDL_LOGGED_WITH_XID => 17
            /// ddl_xid is BigInteger
            let (i, ddl_xid) = le_u64(i)?;
            Ok((i, QueryStatusVar::Q_DDL_LOGGED_WITH_XID(ddl_xid)))
        },
        0x12 => { // Q_DEFAULT_COLLATION_FOR_UTF8MB4 => 18
            let (i, default_collation_for_utf8mb4_number) = le_u16(i)?;
            Ok((i, QueryStatusVar::Q_DEFAULT_COLLATION_FOR_UTF8MB4(default_collation_for_utf8mb4_number)))
        },
        0x13 => { // Q_SQL_REQUIRE_PRIMARY_KEY => 19
            let (i, sql_require_primary_key) = le_u8(i)?;
            Ok((i, QueryStatusVar::Q_SQL_REQUIRE_PRIMARY_KEY(sql_require_primary_key)))
        },
        0x14 => { // Q_DEFAULT_TABLE_ENCRYPTION => 20
            let (i, default_table_encryption) = le_u8(i)?;
            Ok((i, QueryStatusVar::Q_DEFAULT_TABLE_ENCRYPTION(default_table_encryption)))
        },
        0x15 => { // Q_DDL_SKIP_REWRITE => 21
            let (i, binlog_ddl_skip_rewrite) = le_u8(i)?;
            Ok((i, QueryStatusVar::Q_DDL_SKIP_REWRITE(binlog_ddl_skip_rewrite)))
        },
        128 => { // Q_WSREP_SKIP_READONLY_CHECKS => 128
            // https://github.com/alibaba/canal/issues/4940
            // percona 和 mariadb各自扩展mysql binlog的格式后有冲突
            // 需要精确识别一下数据库类型做兼容处理
            // if (compatiablePercona) {
            //     // percona 8.0.31
            //     // Q_WSREP_SKIP_READONLY_CHECKS *start++ = 1;
            //     let (i, _) = le_u8(i)?;
            // } else {
            let (i, when_sec_part) = le_u24(i)?;
            // }
            Ok((i, QueryStatusVar::Q_WSREP_SKIP_READONLY_CHECKS))
        },
        __ => {
            /* That's why you must write status vars in growing order of code  */
            log::error!("Query_log_event has unknown status vars (first has code: {:?}), skipping the rest of them", code);

            unreachable!()
        },
    }
}
