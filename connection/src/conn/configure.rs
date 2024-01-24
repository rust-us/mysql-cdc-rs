use std::cell::RefCell;
use std::sync::Arc;
use binlog::events::checksum_type::ChecksumType;
use common::err::CResult;
use common::err::decode_error::ReError;
use crate::binlog::starting_strategy::StartingStrategy;
use crate::commands::query_command::QueryCommand;
use crate::conn::connection_options::ConnectionOptions;
use crate::conn::packet_channel::PacketChannel;
use crate::packet::check_error_packet;
use crate::packet::response_type::ResponseType;
use crate::packet::result_set_row_packet::ResultSetRowPacket;

pub struct Configure {
    pub options: ConnectionOptions,

}

impl Configure {
    pub fn new(options: ConnectionOptions) -> Self {
        Configure {
            options
        }
    }

    pub fn adjust_starting_position(&mut self, channel: &mut Arc<RefCell<PacketChannel>>) -> CResult<()> {

        if self.options.binlog.is_some() {
            let binlog_opts_copy = self.options.binlog.clone().unwrap();

            if binlog_opts_copy.borrow().starting_strategy != StartingStrategy::FromEnd {
                return Ok(());
            }

            // Ignore if position was read before in case of reconnect.
            if !binlog_opts_copy.borrow().filename.is_empty() {
                return Ok(());
            }
        }

        let command = QueryCommand::new("show master status".to_string());
        channel.borrow_mut().write_packet(&command.serialize()?, 0)?;

        let result_set = self.read_result_set(channel)?;
        if result_set.len() != 1 {
            return Err(ReError::String(
                "Could not read master binlog position.".to_string(),
            ));
        }

        if self.options.binlog.is_some() {
            let a = self.options.binlog.as_mut().unwrap();

            a.borrow_mut().filename = result_set[0].cells[0].clone();
            a.borrow_mut().update_position(result_set[0].cells[1].parse()?);
        }

        Ok(())
    }

    pub fn set_master_heartbeat(&mut self, channel: &mut Arc<RefCell<PacketChannel>>) -> CResult<()> {
        let milliseconds = self.options.heartbeat_interval.as_millis();
        let nanoseconds = milliseconds * 1000 * 1000;
        let query = format!("set @master_heartbeat_period={}", nanoseconds);
        let command = QueryCommand::new(query.to_string());
        channel.borrow_mut().write_packet(&command.serialize()?, 0)?;
        let (packet, _) = channel.borrow_mut().read_packet()?;
        check_error_packet(&packet, "Setting master heartbeat error.")?;
        Ok(())
    }

    pub fn set_master_binlog_checksum(
        &mut self,
        channel: &mut Arc<RefCell<PacketChannel>>,
    ) -> CResult<ChecksumType> {
        let command =
            QueryCommand::new("SET @master_binlog_checksum= @@global.binlog_checksum".to_string());
        channel.borrow_mut().write_packet(&command.serialize()?, 0)?;
        let (packet, _) = channel.borrow_mut().read_packet()?;
        check_error_packet(&packet, "Setting master_binlog_checksum error.")?;

        let command = QueryCommand::new("SELECT @master_binlog_checksum".to_string());
        channel.borrow_mut().write_packet(&command.serialize()?, 0)?;
        let result_set = self.read_result_set(channel)?;

        // When replication is started fake RotateEvent comes before FormatDescriptionEvent.
        // In order to deserialize the event we have to obtain checksum type length in advance.
        Ok(ChecksumType::from_name(&result_set[0].cells[0])?)
    }

    pub fn read_result_set(
        &self,
        channel: &mut Arc<RefCell<PacketChannel>>,
    ) -> CResult<Vec<ResultSetRowPacket>> {
        let (packet, _) = channel.borrow_mut().read_packet()?;
        check_error_packet(&packet, "Reading result set error.")?;

        loop {
            // Skip through metadata
            let (packet, _) = channel.borrow_mut().read_packet()?;
            if packet[0] == ResponseType::END_OF_FILE {
                break;
            }
        }

        let mut result_set = Vec::new();
        loop {
            let (packet, _) = channel.borrow_mut().read_packet()?;
            check_error_packet(&packet, "Query result set error.")?;
            if packet[0] == ResponseType::END_OF_FILE {
                break;
            }
            result_set.push(ResultSetRowPacket::parse(&packet)?);
        }
        Ok(result_set)
    }
}