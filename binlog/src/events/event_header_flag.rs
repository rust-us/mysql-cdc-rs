use serde::Serialize;

// bitflags::bitflags! {
//     /// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__binglog__event__header__flags.html
//     #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
//     #[cfg_attr(feature="serde", serde::Serialize, serde::DeSerialize)]
//     pub struct EventHeaderFlag: u16 {
//         /// If the query depends on the thread (for example: TEMPORARY TABLE).
//         ///
//         /// Currently this is used by mysqlbinlog to know it must print SET @PSEUDO_THREAD_ID=xx; before the query (it would not hurt to print it for every query but this would be slow).
//         const LOG_EVENT_THREAD_SPECIFIC_F = 0x4;
//
//         /// Suppress the generation of 'USE' statements before the actual statement.
//         ///
//         /// This flag should be set for any events that does not need the current database set to function correctly. Most notable cases are 'CREATE DATABASE' and 'DROP DATABASE'.
//         ///
//         /// This flags should only be used in exceptional circumstances, since it introduce a significant change in behaviour regarding the replication logic together with the flags –binlog-do-db and –replicated-do-db.
//         const LOG_EVENT_SUPPRESS_USE_F = 0x8;
//
//         /// Artificial events are created arbitrarily and not written to binary log.
//         ///
//         /// These events should not update the master log position when slave SQL thread executes them.
//         const LOG_EVENT_ARTIFICIAL_F = 0x20;
//
//         /// Events with this flag set are created by slave IO thread and written to relay log.
//         const LOG_EVENT_RELAY_LOG_F = 0x40;
//
//         /// For an event, 'e', carrying a type code, that a slave, 's', does not recognize, 's' will check 'e' for LOG_EVENT_IGNORABLE_F, and if the flag is set, then 'e' is ignored.
//         ///
//         /// Otherwise, 's' acknowledges that it has found an unknown event in the relay log.
//         const LOG_EVENT_IGNORABLE_F = 0x80;
//
//         /// Events with this flag are not filtered
//         ///
//         /// (e.g.  on the current database) and are always written to the binary log regardless of filters.
//         const LOG_EVENT_NO_FILTER_F = 0x100;
//
//         /// MTS: group of events can be marked to force its execution in isolation from any other Workers.
//         ///
//         /// So it's a marker for Coordinator to memorize and perform necessary operations in order to guarantee no interference from other Workers. The flag can be set ON only for an event that terminates its group. Typically that is done for a transaction that contains a query accessing more than OVER_MAX_DBS_IN_EVENT_MTS databases.
//         const LOG_EVENT_MTS_ISOLATE_F = 0x200;
//     }
// }
//
// impl<I: InputBuf> Decode<I> for EventHeaderFlag {
//     fn decode(input: &mut I) -> Result<Self, DecodeError> {
//         // flag 解析为一个 u16，但它实际上是一个 bit_flag
//         let flags = Int2::decode(input)?;
//         // 但它实际上是一个 bit_flag, 所以根据 Binlog Event Flag 对 u16 进行右移得到真正的 flag。
//         Self::from_bits(flags.int()).ok_or(DecodeError::InvalidData)
//     }
// }
//


/////////////////////////////////////
///  EventHeaderFlag  EventFlag
/////////////////////////////////////
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct EventFlag {
    in_use: bool,
    forced_rotate: bool,
    thread_specific: bool,
    suppress_use: bool,
    update_table_map_version: bool,
    artificial: bool,
    relay_log: bool,
    ignorable: bool,
    no_filter: bool,
    mts_isolate: bool,
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
