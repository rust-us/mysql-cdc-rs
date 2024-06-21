use serde::Serialize;

/////////////////////////////////////
/// EventHeaderFlag  EventFlag
/// flag 解析为一个 u16，但它实际上是一个 bit_flag
///
/// @see https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__binglog__event__header__flags.html
/////////////////////////////////////
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct EventFlag {
    pub in_use: bool,

    pub forced_rotate: bool,

    /// LOG_EVENT_THREAD_SPECIFIC_F   0x4
    /// If the query depends on the thread (for example: TEMPORARY TABLE).
    /// Currently this is used by mysqlbinlog to know it must print SET @PSEUDO_THREAD_ID=xx; before the query (it would not hurt to print it for every query but this would be slow).
    pub thread_specific: bool,

    /// LOG_EVENT_SUPPRESS_USE_F   0x8
    /// Suppress the generation of 'USE' statements before the actual statement.
    /// This flag should be set for any events that does not need the current database set to function correctly. Most notable cases are 'CREATE DATABASE' and 'DROP DATABASE'.
    ///
    /// This flags should only be used in exceptional circumstances, since it introduce a significant change in behaviour regarding the replication logic together with the flags –binlog-do-db and –replicated-do-db.
    pub suppress_use: bool,

    pub update_table_map_version: bool,

    /// LOG_EVENT_ARTIFICIAL_F   0x20
    /// Artificial events are created arbitrarily and not written to binary log.
    /// These events should not update the master log position when slave SQL thread executes them.
    pub artificial: bool,

    /// LOG_EVENT_RELAY_LOG_F   0x40
    /// Events with this flag set are created by slave IO thread and written to relay log
    pub relay_log: bool,

    /// LOG_EVENT_IGNORABLE_F   0x80
    /// For an event, 'e', carrying a type code, that a slave, 's', does not recognize,
    /// 's' will check 'e' for LOG_EVENT_IGNORABLE_F, and if the flag is set, then 'e' is ignored.
    /// Otherwise, 's' acknowledges that it has found an unknown event in the relay log.
    pub ignorable: bool,

    /// LOG_EVENT_NO_FILTER_F   0x100
    /// Events with this flag are not filtered.
    /// on the current database) and are always written to the binary log regardless of filters.
    pub no_filter: bool,

    /// LOG_EVENT_MTS_ISOLATE_F   0x200
    /// MTS: group of events can be marked to force its execution in isolation from any other Workers.
    /// So it's a marker for Coordinator to memorize and perform necessary operations in order to guarantee
    /// no interference from other Workers. The flag can be set ON only for an event that terminates its group.
    /// Typically that is done for a transaction that contains a query accessing
    /// more than OVER_MAX_DBS_IN_EVENT_MTS databases.
    pub mts_isolate: bool,
}

impl Default for EventFlag {
    fn default() -> Self {
        Self {
            in_use: false,
            forced_rotate: false,
            thread_specific: false,
            suppress_use: false,
            update_table_map_version: false,
            artificial: false,
            relay_log: false,
            ignorable: false,
            no_filter: false,
            mts_isolate: false,
        }
    }
}

impl From<u16> for EventFlag {
    fn from(f: u16) -> Self {
         EventFlag {
             in_use: (f >> 0) % 2 == 1,
             forced_rotate: (f >> 1) % 2 == 1,
             thread_specific: (f >> 2) % 2 == 1,
             suppress_use: (f >> 3) % 2 == 1,
             update_table_map_version: (f >> 4) % 2 == 1,
             artificial: (f >> 5) % 2 == 1,
             relay_log: (f >> 6) % 2 == 1,
             ignorable: (f >> 7) % 2 == 1,
             no_filter: (f >> 8) % 2 == 1,
             mts_isolate: (f >> 9) % 2 == 1,
         }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
    }
}
