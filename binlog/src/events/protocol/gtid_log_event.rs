use std::collections::HashMap;
use crate::events::event::Event;
use crate::events::event_header::Header;
use crate::events::log_event::LogEvent;
use nom::number::complete::be_u64;
use nom::{
    bytes::complete::take,
    combinator::map,
    number::complete::{le_i64, le_u32, le_u8},
    IResult,
};
use serde::Serialize;
use uuid::Uuid;
use crate::events::event_raw::HeaderRef;

pub const LOGICAL_TIMESTAMP_TYPE_CODE: u8 = 2;

/// https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/include/control_events.h#L1048-L1056
/// (equals AnonymousGtidLogEvent)
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct GtidLogEvent {
    header: Header,

    /// 记录binlog格式
    /// 如果gtid_flags值为1，表示binlog中可能有以statement方式记录的binlog。 此时 commit_flag 为 true
    /// 如果为0表示，binlog中只有以row格式记录的binlog。 此时 commit_flag 为 false
    pub commit_flag: bool,

    /// 16字节
    pub sid: String,
    /// 8字节, transaction_id， Long
    pub gno: String,

    /// logical_timestamp_typecode
    pub lt_type: u8,

    pub last_committed: i64,
    pub sequence_number: i64,
}

impl GtidLogEvent {
    pub fn new(
        header: Header,
        commit_flag: bool,
        sid: String,
        gno: String,
        lt_type: u8,
        last_committed: i64,
        sequence_number: i64,
    ) -> Self {
        GtidLogEvent {
            header,
            commit_flag,
            sid,
            gno,
            lt_type,
            last_committed,
            sequence_number,
        }
    }

    pub fn parse<'a>(input: &'a [u8], header: HeaderRef) -> IResult<&'a [u8], GtidLogEvent> {
        let (
            i,
            (
                commit_flag,
                source_id,
                transaction_id,
                lt_type,
                last_committed,
                sequence_number,
                checksum,
            ),
        ) = GtidLogEvent::parse_events_gtid(input, header.clone())?;

        let e = GtidLogEvent {
            header: Header::copy_and_get(header, checksum, HashMap::new()),
            commit_flag,
            sid: source_id,
            gno: transaction_id,
            lt_type,
            last_committed,
            sequence_number,
        };

        Ok((i, e))
    }

    pub fn parse_events_gtid<'a>(
        input: &'a [u8],
        header: HeaderRef,
    ) -> IResult<&'a [u8], (bool, String, String, u8, i64, i64, u32)> {
        // 记录binlog格式:
        // 如果gtid_flags值为1，表示binlog中可能有以statement方式记录的binlog
        // 如果为0表示，binlog中只有以row格式记录的binlog
        let (i, commit_flag) = map(le_u8, |t: u8| t != 0)(input)?;

        let (i, high) = be_u64(i)?;
        let (i, low) = be_u64(i)?;
        let mut source_id_uuid = Uuid::default();
        if high == 0 && low == 0 {
            source_id_uuid = Uuid::default();
        } else {
            source_id_uuid = Uuid::from_u64_pair(high, low);
            // let (i, source_id) = map(take(16usize), |s: &[u8]| {
            //     format!(
            //         "{}-{}-{}-{}-{}",
            //         s[..4].iter().fold(String::new(), |mut acc, i| {
            //             acc.push_str(&i.to_string());
            //             acc
            //         }),
            //         s[4..6].iter().fold(String::new(), |mut acc, i| {
            //             acc.push_str(&i.to_string());
            //             acc
            //         }),
            //         s[6..8].iter().fold(String::new(), |mut acc, i| {
            //             acc.push_str(&i.to_string());
            //             acc
            //         }),
            //         s[8..10].iter().fold(String::new(), |mut acc, i| {
            //             acc.push_str(&i.to_string());
            //             acc
            //         }),
            //         s[10..].iter().fold(String::new(), |mut acc, i| {
            //             acc.push_str(&i.to_string());
            //             acc
            //         }),
            //     )
            // })(i)?;
        }
        let source_id = source_id_uuid.to_string();

        let (i, transaction_id) = map(take(8usize), |s: &[u8]| {
            s.iter().fold(String::new(), |mut acc, i| {
                acc.push_str(&i.to_string());
                acc
            })
        })(i)?;

        let (i, lt_type) = le_u8(i)?;

        if lt_type == LOGICAL_TIMESTAMP_TYPE_CODE {
            let (i, last_committed) = le_i64(i)?;
            let (i, sequence_number) = le_i64(i)?;

            let remain_len = header.borrow().event_length - (19 + 1 + 16 + 8 + 1 + 8 + 8);
            if remain_len > 4 {
                let (i, _) = map(take((remain_len - 4) as u8), |s: &[u8]| s.to_vec())(i)?;
                // eq ==>
                // let (i, ignore_a) = le_u32(i)?;
                // let (i, ignore_b) = le_u32(i)?;
                // let (i, ignore_c) = le_u32(i)?;

                let (i, checksum) = le_u32(i)?;

                Ok((
                    i,
                    (
                        commit_flag,
                        source_id,
                        transaction_id,
                        lt_type,
                        last_committed,
                        sequence_number,
                        checksum,
                    ),
                ))
            } else {
                let (i, checksum) = le_u32(i)?;

                Ok((
                    i,
                    (
                        commit_flag,
                        source_id,
                        transaction_id,
                        lt_type,
                        last_committed,
                        sequence_number,
                        checksum,
                    ),
                ))
            }
        } else {
            let (i, checksum) = le_u32(i)?;

            Ok((
                i,
                (
                    commit_flag,
                    source_id,
                    transaction_id,
                    lt_type,
                    0,
                    0,
                    checksum,
                ),
            ))
        }
    }

    pub fn get_last_committed(&self) -> i64 {
        self.last_committed
    }

    pub fn get_sequence_number(&self) -> i64 {
        self.sequence_number
    }

    pub fn get_gtid_str(&self) -> String {
        let sid = self.sid.clone();
        let gno = self.gno.clone();

        format!("{}:{}", sid, gno)
    }
}

// impl LogEvent for GtidLogEvent {
//     fn get_type_name(&self) -> String {
//         "GtidLogEvent".to_string()
//     }
// }
