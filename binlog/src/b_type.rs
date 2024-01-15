use tracing::error;

pub const C_ENUM_END_EVENT: isize = 171;

#[derive(Debug)]
pub enum BinlogType {
    /// 基于SQL语句的复制（statement-based replication, SBR）, 每一条会修改数据的sql都会记录在binlog中。
    ///
    /// 优点：不需要记录每一行的变化，减少了binlog日志量，节约了IO, 提高了性能。
    ///
    /// 缺点：由于记录的只是执行语句，为了这些语句能在slave上正确运行，
    /// 因此还必须记录每条语句在执行的时候的一些相关信息，
    /// 以保证所有语句能在slave得到和在master端执行的时候相同的结果。
    /// 另外mysql的复制，像一些特定函数的功能，slave可与master上要保持一致会有很多相关问题。
    ///
    /// 相比row能节约多少性能与日志量，这个取决于应用的SQL情况，
    /// 正常同一条记录修改或者插入row格式所产生的日志量还小鱼statement产生的日志量，
    /// 但是考虑到如果带条件的update操作，以及整表删除，alter表等操作，row格式会产生大量日志，
    /// 因此在考虑是否使用row格式日志时应该根据应用的实际情况，
    /// 其所产生的日志量会增加多少，以及带来的IO性能问题
    Statement,

    /// 基于行的复制（row-based replication, RBR）
    ///
    /// 5.1.5版本的MySQL才开始支持row level的复制,它不记录sql语句上下文相关信息，仅保存哪条记录被修改。
    ///
    /// 优点： binlog中可以不记录执行的sql语句的上下文相关的信息，仅需要记录那一条记录被修改成什么了。
    /// 所以row的日志内容会非常清楚的记录下每一行数据修改的细节。
    /// 而且不会出现某些特定情况下的存储过程，或function，以及trigger的调用和触发无法被正确复制的问题.
    ///
    /// 缺点:所有的执行的语句当记录到日志中的时候，都将以每行记录的修改来记录，这样可能会产生大量的日志内容。
    ///
    /// 新版本的MySQL中对row level模式也被做了优化，并不是所有的修改都会以row level来记录，
    /// 像遇到表结构变更的时候就会以statement模式来记录，
    /// 如果sql语句确实就是update或者delete等修改数据的语句，那么还是会记录所有行的变更。
    Row,

    /// 混合模式复制（mixed-based replication, MBR）
    ///
    /// 从5.1.8版本开始，MySQL提供了Mixed格式，实际上就是Statement与Row的结合。
    /// 在Mixed模式下，一般的语句修改使用statment格式保存binlog，
    /// 如一些函数，statement无法完成主从复制的操作，则采用row格式保存binlog，
    /// MySQL会根据执行的每一条具体的sql语句来区分对待记录的日志形式，也就是在Statement和Row之间选择一种。
    Mixed,

}

///
/// @see  https://dev.mysql.com/doc/dev/mysql-server/latest/namespacemysql_1_1binlog_1_1event.html#a4a991abea842d4e50cbee0e490c28ceea7b14f67ef2d7aa312bc446dd11e3de03
///
#[derive(Debug)]
pub enum LogEventType {
    /// Every time you update this enum (when you add a type),
    /// you have to fix Format_description_event::Format_description_event().
    UNKNOWN_EVENT = 0,

    ///  START_EVENT_V3事件 在version 4 中被FORMAT_DESCRIPTION_EVENT是binlog替代
    /// This is sent only by MySQL <=4.x
    START_EVENT_V3 = 1,

    /// 记录一条query语句，在基于语句的复制和基于行的复制都会有。
    QUERY_EVENT = 2,
    /// MySQL停止时，在文件尾加入STOP_EVENT
    STOP_EVENT = 3,
    /// 二进制日志更换一个新文件，可能因为文件大小达到限制，或者是mysql重启，亦或者是调用了flush logs命令。
    ROTATE_EVENT = 4,
    /// 在statement时使用到，用于自增类型auto_increment.
    INTVAR_EVENT = 5,
    LOAD_EVENT = 6,
    SLAVE_EVENT = 7,
    CREATE_FILE_EVENT = 8,
    APPEND_BLOCK_EVENT = 9,
    EXEC_LOAD_EVENT = 10,
    DELETE_FILE_EVENT = 11,

    /// NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer sql_ex,
    /// allowing multibyte TERMINATED BY etc; both types share the same class (Load_event)
    NEW_LOAD_EVENT = 12,
    RAND_EVENT = 13,
    USER_VAR_EVENT = 14,

    /// FORMAT_DESCRIPTION_EVENT是binlog version 4中为了取代之前版本中的START_EVENT_V3事件而引入的。
    /// 它是binlog文件中的第一个事件，而且，该事件只会在binlog中出现一次。MySQL根据FORMAT_DESCRIPTION_EVENT的定义来解析其它事件。
    /// 它通常指定了MySQL Server的版本，binlog的版本，该binlog文件的创建时间。
    /// eg:
    /// <code>
    /// # at 4
    //  #200731  6:24:55 server id 1  end_log_pos 123 CRC32 0xf0bd8e51
    /// </code>
    FORMAT_DESCRIPTION_EVENT = 15,

    /// Commit事件
    XID_EVENT = 16,
    BEGIN_LOAD_QUERY_EVENT = 17,
    EXECUTE_LOAD_QUERY_EVENT = 18,

    /// ROW EVENT之前产生，为的是对ROW EVENT解析提供依据。
    TABLE_MAP_EVENT = 19,

    /// The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are therefore obsolete.
    PRE_GA_WRITE_ROWS_EVENT = 20,
    PRE_GA_UPDATE_ROWS_EVENT = 21,
    PRE_GA_DELETE_ROWS_EVENT = 22,

    /// 统称为 ROW EVENT, 只有在基于row的复制方式下才会产生。
    /// 包含了要插入的数据. The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
    WRITE_ROWS_EVENT_V1 = 23,
    /// 包含了修改前的值，也包含了修改后的值
    UPDATE_ROWS_EVENT_V1 = 24,
    /// 包含了需要删除行前的值
    DELETE_ROWS_EVENT_V1 = 25,

    /// Something out of the ordinary happened on the master
    INCIDENT_EVENT = 26,

    /// Heartbeat event to be send by master at its idle time to ensure master's online status to slave
    HEARTBEAT_LOG_EVENT = 27,

    /// In some situations, it is necessary to send over ignorable data to the slave:
    /// data that a slave can handle in case there is code for handling it, but which can be ignored if it is not recognized.
    IGNORABLE_LOG_EVENT = 28,
    ROWS_QUERY_LOG_EVENT = 29,

    /// Version 2 of the Row events
    WRITE_ROWS_EVENT = 30,
    UPDATE_ROWS_EVENT = 31,
    DELETE_ROWS_EVENT = 32,

    GTID_LOG_EVENT = 33,
    ANONYMOUS_GTID_LOG_EVENT = 34,

    PREVIOUS_GTIDS_LOG_EVENT = 35,

    ///
    TRANSACTION_CONTEXT_EVENT= 36,

    VIEW_CHANGE_EVENT= 37,

    /* Prepared XA transaction terminal event similar to Xid */
    XA_PREPARE_LOG_EVENT= 38,

    /// Extension of UPDATE_ROWS_EVENT, allowing partial values according to binlog_row_value_options.
    PARTIAL_UPDATE_ROWS_EVENT = 39,

    TRANSACTION_PAYLOAD_EVENT = 40,
    HEARTBEAT_LOG_EVENT_V2 = 41,

    /// Add new events here - right above this comment!
    /// Existing events (except ENUM_END_EVENT) should never change their numbers
    // ENUM_END_EVENT = 171,
    MYSQL_ENUM_END_EVENT = 42,

    /* New MySQL/Sun events are to be added right above this comment */
    MYSQL_EVENTS_END = 49,

    /* New Maria event numbers start from here */
    ANNOTATE_ROWS_EVENT = 160,

    /// Binlog checkpoint event. Used for XA crash recovery on the master, not
    /// used in replication. A binlog checkpoint event specifies a binlog file
    /// such that XA crash recovery can start from that file - and it is
    /// guaranteed to find all XIDs that are prepared in storage engines but not
    /// yet committed.
    BINLOG_CHECKPOINT_EVENT = 161,

    /// Gtid event. For global transaction ID, used to start a new event group,
    /// instead of the old BEGIN query event, and also to mark stand-alone
    /// events.
    GTID_EVENT = 162,

    /// Gtid list event. Logged at the start of every binlog, to record the
    /// current replication state. This consists of the last GTID seen for each
    /// replication domain.
    GTID_LIST_EVENT = 163,
    START_ENCRYPTION_EVENT = 164,

    /// mariadb 10.10.1
    /// Compressed binlog event. Note that the order between WRITE/UPDATE/DELETE
    /// events is significant; this is so that we can convert from the compressed to
    /// the uncompressed event type with (type-WRITE_ROWS_COMPRESSED_EVENT +
    /// WRITE_ROWS_EVENT) and similar for _V1.
    QUERY_COMPRESSED_EVENT = 165,
    WRITE_ROWS_COMPRESSED_EVENT_V1 = 166,
    UPDATE_ROWS_COMPRESSED_EVENT_V1 = 167,
    DELETE_ROWS_COMPRESSED_EVENT_V1 = 168,
    WRITE_ROWS_COMPRESSED_EVENT = 169,
    UPDATE_DELETE_ROWS_COMPRESSED_EVENT = 170,

    /** end marker */
    ENUM_END_EVENT = C_ENUM_END_EVENT,
}

impl From<u8> for LogEventType {
    fn from(b_type: u8) -> Self {
        match b_type {
            0x00 => LogEventType::UNKNOWN_EVENT,
            0x01 => LogEventType::START_EVENT_V3,
            0x02 => LogEventType::QUERY_EVENT,
            0x03 => LogEventType::STOP_EVENT,
            0x04 => LogEventType::ROTATE_EVENT,
            0x05 => LogEventType::INTVAR_EVENT,
            0x06 => LogEventType::LOAD_EVENT,
            0x07 => LogEventType::SLAVE_EVENT,
            0x08 => LogEventType::CREATE_FILE_EVENT,
            0x09 => LogEventType::APPEND_BLOCK_EVENT,
            0x0a => LogEventType::EXEC_LOAD_EVENT,     // 10
            0x0b => LogEventType::DELETE_FILE_EVENT,   // 11
            0x0c => LogEventType::NEW_LOAD_EVENT,      // 12
            0x0d => LogEventType::RAND_EVENT,          // 13
            0x0e => LogEventType::USER_VAR_EVENT,      // 14
            0x0f => LogEventType::FORMAT_DESCRIPTION_EVENT, // 15
            0x10 => LogEventType::XID_EVENT,           // 16
            0x11 => LogEventType::BEGIN_LOAD_QUERY_EVENT,      // 17
            0x12 => LogEventType::EXECUTE_LOAD_QUERY_EVENT,    // 18
            /// ROW EVENT之前产生，为的是对ROW EVENT解析提供依据。
            0x13 => LogEventType::TABLE_MAP_EVENT,     // 19
            0x14 => LogEventType::PRE_GA_WRITE_ROWS_EVENT,
            0x15 => LogEventType::PRE_GA_UPDATE_ROWS_EVENT,
            0x16 => LogEventType::PRE_GA_DELETE_ROWS_EVENT,
            0x17 => LogEventType::WRITE_ROWS_EVENT_V1,
            0x18 => LogEventType::UPDATE_ROWS_EVENT_V1,
            0x19 => LogEventType::DELETE_ROWS_EVENT_V1,
            0x1a => LogEventType::INCIDENT_EVENT,      // 26
            0x1b => LogEventType::HEARTBEAT_LOG_EVENT,     // 27
            0x1c => LogEventType::IGNORABLE_LOG_EVENT,     // 28
            0x1d => LogEventType::ROWS_QUERY_LOG_EVENT,     // 29
            0x1e => LogEventType::WRITE_ROWS_EVENT, // 30
            0x1f => LogEventType::UPDATE_ROWS_EVENT,// 31
            0x20 => LogEventType::DELETE_ROWS_EVENT,// 32
            0x21 => LogEventType::GTID_LOG_EVENT,// 33
            0x22 => LogEventType::ANONYMOUS_GTID_LOG_EVENT,// 34
            0x23 => LogEventType::PREVIOUS_GTIDS_LOG_EVENT,// 35
            36 => LogEventType::TRANSACTION_CONTEXT_EVENT,
            37 => LogEventType::VIEW_CHANGE_EVENT,
            38 => LogEventType::XA_PREPARE_LOG_EVENT,
            39 => LogEventType::PARTIAL_UPDATE_ROWS_EVENT,
            40 => LogEventType::TRANSACTION_PAYLOAD_EVENT,
            // @see https://dev.mysql.com/doc/dev/mysql-server/latest/namespacemysql_1_1binlog_1_1event.html#a4a991abea842d4e50cbee0e490c28ceea1b1312ed0f5322b720ab2b957b0e9999
            41 => LogEventType::HEARTBEAT_LOG_EVENT_V2,
            42 => LogEventType::MYSQL_ENUM_END_EVENT,// 42
            171 => LogEventType::ENUM_END_EVENT,// 171

            t @ _ => {
                error!("unexpected event type: {:x}", t);
                unreachable!();
            }
        }
    }
}

impl LogEventType {
    pub fn as_val(self) -> usize {
        self as usize
    }
}