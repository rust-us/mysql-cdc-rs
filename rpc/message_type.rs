use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(IntoPrimitive, Debug, Copy, Clone, TryFromPrimitive)]
#[repr(i32)]
pub enum MessageType {

    Other = -1,

    Heartbeat = 6,
    HeartbeatResponse = 7,

    ResponseCommand = 10,

}

