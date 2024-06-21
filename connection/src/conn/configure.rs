use std::cell::RefCell;
use std::sync::Arc;

use binlog::events::checksum_type::ChecksumType;
use common::err::decode_error::ReError;
use common::err::CResult;

use crate::binlog::starting_strategy::StartingStrategy;
use crate::commands::query_command::QueryCommand;
use crate::conn::connection_options::{ConnectionOptions, ConnectionOptionsRef};
use crate::conn::packet_channel::PacketChannel;
use crate::packet::check_error_packet;
use crate::packet::response_type::ResponseType;
use crate::packet::result_set_row_packet::ResultSetRowPacket;

/// show master status 命令查询到的binlog信息表相关column的index
/// |File|Position|Binlog_Do_DB|Binlog_Ignore_DB|Executed_Gtid_Set|
/// |----|--------|------------|----------------|-----------------|
/// |binlog.001375|15093139|   |                |                 |
/// File字段序号
const BINLOG_MASTER_STATUS_COLUMN_FILENAME_INDEX: usize = 0;
/// Position字段序号
const BINLOG_MASTER_STATUS_COLUMN_POSITION_INDEX: usize = 1;
/// GTID字段序号
const BINLOG_MASTER_STATUS_COLUMN_GTID_INDEX: usize = 4;

/// SHOW BINARY LOGS 命令查询到的binlog信息表相关column的index
/// |Log_name|File_size|Encrypted|
/// |--------|---------|---------|
/// |binlog.001365|180 |       No|
/// File字段序号
const BINLOG_SHOW_LOGS_COLUMN_LOG_NAME_INDEX: usize = 0;

/// SHOW VARIABLES 命令查询结果相关column的index
/// |Variable_name|Value|
/// |-------------|-----|
/// |server_id    |    1|
const SHOW_VARIABLES_COLUMN_VALUE_INDEX: usize = 1;

/// SELECT @@xxx 命令查询结果相关column的index
/// |@@global.binlog_checksum|
/// |------------------------|
/// |CRC32|
const SELECT_VARIABLES_COLUMN_VALUE_INDEX: usize = 0;

#[derive(Debug)]
pub struct Configure {
    pub options: ConnectionOptionsRef,
}

impl Configure {
    pub fn new(options: ConnectionOptions) -> Self {
        Configure {
            options: Arc::new(RefCell::new(options)),
        }
    }

    pub fn adjust_starting_position(
        &mut self,
        channel: &mut Arc<RefCell<PacketChannel>>,
    ) -> CResult<()> {
        if self.options.as_ref().borrow().binlog.is_some() {
            let binlog_opts_copy = self.options.as_ref().borrow().binlog.clone().unwrap();

            if binlog_opts_copy.borrow().starting_strategy != StartingStrategy::FromEnd {
                return Ok(());
            }

            // Ignore if position was read before in case of reconnect.
            if !binlog_opts_copy.borrow().filename.is_empty() {
                return Ok(());
            }
        }

        // query end position
        let command = QueryCommand::new("show master status".to_string());
        channel
            .borrow_mut()
            .write_packet(&command.serialize()?, 0)?;

        let result_set = self.read_result_set(channel)?;
        if result_set.len() != 1 {
            return Err(ReError::String(
                "Could not read master binlog position.".to_string(),
            ));
        }

        if self.options.as_ref().borrow().binlog.is_some() {
            let filename: String = result_set[0].cells[BINLOG_MASTER_STATUS_COLUMN_FILENAME_INDEX]
                .clone()
                .ok_or(ReError::MysqlQueryErr(String::from(
                    "Can not get binlog filename from 'show master status'",
                )))?;
            let pos = result_set[0].cells[BINLOG_MASTER_STATUS_COLUMN_POSITION_INDEX]
                .clone()
                .ok_or(ReError::MysqlQueryErr(String::from(
                    "Can not get binlog position from 'show master status'",
                )))?;

            self.options
                .borrow_mut()
                .update_binlog_position(filename, pos.parse()?);
        }

        Ok(())
    }

    pub fn set_master_heartbeat(
        &mut self,
        channel: &mut Arc<RefCell<PacketChannel>>,
    ) -> CResult<()> {
        let milliseconds = self
            .options
            .as_ref()
            .borrow()
            .heartbeat_interval
            .as_millis();
        let nanoseconds = milliseconds * 1000 * 1000;
        let query = format!("set @master_heartbeat_period={}", nanoseconds);
        let command = QueryCommand::new(query.to_string());
        channel
            .borrow_mut()
            .write_packet(&command.serialize()?, 0)?;
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
        channel
            .borrow_mut()
            .write_packet(&command.serialize()?, 0)?;
        let (packet, _) = channel.borrow_mut().read_packet()?;
        check_error_packet(&packet, "Setting master_binlog_checksum error.")?;

        let command = QueryCommand::new("SELECT @master_binlog_checksum".to_string());
        channel
            .borrow_mut()
            .write_packet(&command.serialize()?, 0)?;
        let result_set = self.read_result_set(channel)?;

        // When replication is started fake RotateEvent comes before FormatDescriptionEvent.
        // In order to deserialize the event we have to obtain checksum type length in advance.
        Ok(ChecksumType::from_name(
            &result_set[0].cells[0].clone().unwrap_or_default(),
        )?)
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
