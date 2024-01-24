use common::err::CResult;
use common::err::decode_error::ReError;
use crate::packet::error_packet::ErrorPacket;
use crate::packet::response_type::ResponseType;

pub mod auth_switch_packet;
pub mod handshake_packet;
pub mod response_type;
pub mod end_of_file_packet;
pub mod error_packet;
pub mod result_set_row_packet;

pub fn check_error_packet(packet: &[u8], message: &str) -> CResult<()> {
    if packet[0] == ResponseType::ERROR {
        let error = ErrorPacket::parse(&packet[1..])?;
        let message = format!("{} {:?}", message, error).to_string();
        return Err(ReError::String(message));
    }

    return Ok(());
}
