use serde::Serialize;

pub mod protocol;

pub mod event;
pub mod event_c;
pub mod rows;
pub mod event_header_flag;
pub mod event_header;
pub mod query;
pub mod log_event;
pub mod log_context;
pub mod log_position;

pub mod event_factory;

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum IntVarEventType {
    InvalidIntEvent,
    LastInsertIdEvent,
    InsertIdEvent,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct EmptyFlags {
    pub field_term_empty: bool,
    pub enclosed_empty: bool,
    pub line_term_empty: bool,
    pub line_start_empty: bool,
    pub escape_empty: bool,
}


#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum DupHandlingFlags {
    Error,
    Ignore,
    Replace,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum IncidentEventType {
    None,
    LostEvents,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub enum UserVarType {
    STRING = 0,
    REAL = 1,
    INT = 2,
    ROW = 3,
    DECIMAL = 4,
    VALUE_TYPE_COUNT = 5,
    Unknown,
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct OptFlags {
    pub dump_file: bool,
    pub opt_enclosed: bool,
    pub replace: bool,
    pub ignore: bool,
}