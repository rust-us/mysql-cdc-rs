
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
    UnknownEvent = 0,
    StartEventV3 = 1,
    /// 记录一条query语句，在基于语句的复制和基于行的复制都会有。
    QueryEvent = 2,
    /// MySQL停止时，在文件尾加入STOP_EVENT
    StopEvent = 3,
    /// 二进制日志更换一个新文件，可能因为文件大小达到限制，或者是mysql重启，亦或者是调用了flush logs命令。
    RotateEvent = 4,
    /// 在statement时使用到，用于自增类型auto_increment.
    IntvarEvent = 5,
    LoadEvent = 6,
    SlaveEvent = 7,
    CreateFileEvent = 8,
    AppendBlockEvent = 9,
    ExecLoadEvent = 10,
    DeleteFileEvent = 11,
    NewLoadEvent = 12,
    RandEvent = 13,
    UserVarEvent = 14,
    /// MySQL根据其定义来解析其他事件
    FormatDescriptionEvent = 15,
    /// Commit事件
    XidEvent = 16,
    BeginLoadQueryEvent = 17,
    ExecuteLoadQueryEvent = 18,
    /// ROW EVENT之前产生，为的是对ROW EVENT解析提供依据。
    TableMapEvent = 19,
    PreGaWriteRowsEvent = 20,
    PreGaUpdateRowsEvent = 21,
    PreGaDeleteRowsEvent = 22,

    /// 统称为 ROW EVENT, 只有在基于row的复制方式下才会产生。
    /// 包含了要插入的数据
    WriteRowsEvent = 23,
    /// 包含了修改前的值，也包含了修改后的值
    UpdateRowsEvent = 24,
    /// 包含了需要删除行前的值
    DeleteRowsEvent = 25,

    IncidentEvent = 26,
    HeartbeatLogEvent = 27,
    IgnorableLogEvent = 28,
    RowsQueryLogEvent = 29,
    WriteRowsEventV2 = 30,
    UpdateRowsEventV2 = 31,
    DeleteRowsEventV2 = 32,
    GtidLogEvent = 33,
    AnonymousGtidLogEvent = 34,
    PreviousGtidsLogEvent = 35,

}