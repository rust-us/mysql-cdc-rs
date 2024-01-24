use serde::Serialize;

#[derive(Clone, Copy, Serialize, PartialEq, Debug)]
pub enum StartingStrategy {
    FromStart,
    FromEnd,
    FromPosition,
    FromGtid,
}
