
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