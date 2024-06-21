/// mysql服务器status flag, 握手时获得，ok包更新

/// Is raised when a multi-statement transaction has been started, either explicitly,
/// by means of BEGIN or COMMIT AND CHAIN, or implicitly, by the first transactional
/// statement, when autocommit=off.
pub(crate) const SERVER_STATUS_IN_TRANS: u16 = 0x0001;

/// Server in auto_commit mode.
pub(crate) const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;

/// Multi query - next query exists.
pub(crate) const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;

pub(crate) const SERVER_STATUS_NO_GOOD_INDEX_USED: u16 = 0x0010;

pub(crate) const SERVER_STATUS_NO_INDEX_USED: u16 = 0x0020;

/// The server was able to fulfill the clients request and opened a read-only
/// non-scrollable cursor for a query. This flag comes in reply to COM_STMT_EXECUTE
/// and COM_STMT_FETCH commands. Used by Binary Protocol Resultset to signal that
/// COM_STMT_FETCH must be used to fetch the row-data.
pub(crate) const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;

/// This flag is sent when a read-only cursor is exhausted, in reply to
/// COM_STMT_FETCH command.
pub(crate) const SERVER_STATUS_LAST_ROW_SENT: u16 = 0x0080;

/// A database was dropped.
pub(crate) const SERVER_STATUS_DB_DROPPED: u16 = 0x0100;

pub(crate) const SERVER_STATUS_NO_BACKSLASH_ESCAPES: u16 = 0x0200;

/// Sent to the client if after a prepared statement reprepare we discovered
/// that the new statement returns a different number of result set columns.
pub(crate) const SERVER_STATUS_METADATA_CHANGED: u16 = 0x0400;

pub(crate) const SERVER_QUERY_WAS_SLOW: u16 = 0x0800;

/// To mark ResultSet containing output parameter values.
pub(crate) const SERVER_PS_OUT_PARAMS: u16 = 0x1000;

/// Set at the same time as SERVER_STATUS_IN_TRANS if the started multi-statement
/// transaction is a read-only transaction. Cleared when the transaction commits
/// or aborts. Since this flag is sent to clients in OK and EOF packets, the flag
/// indicates the transaction status at the end of command execution.
pub(crate) const SERVER_STATUS_IN_TRANS_READONLY: u16 = 0x2000;

/// This status flag, when on, implies that one of the state information has
/// changed on the server because of the execution of the last statement.
pub(crate) const SERVER_SESSION_STATE_CHANGED: u16 = 0x4000;

#[derive(Debug)]
pub struct StatusFlags {
    status_flags: u16,
}

impl StatusFlags {
    pub fn new(status_flags: u16) -> Self {
        StatusFlags { status_flags }
    }

    pub fn empty() -> Self {
        StatusFlags::new(0)
    }
    pub fn contains(&self, status_flag: u16) -> bool {
        (self.status_flags & status_flag) != 0
    }
}
