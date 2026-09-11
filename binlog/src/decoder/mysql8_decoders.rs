use std::collections::HashMap;
use std::io::Cursor;
use common::err::decode_error::{ReError, ErrorContext};
use crate::b_type::LogEventType;
use crate::events::binlog_event::BinlogEvent;
use crate::events::event_raw::HeaderRef;
use crate::events::log_context::LogContextRef;
use crate::events::declare::log_event::LogEvent;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::decoder::event_decoder_registry::{EventDecoder, DecoderPriority};
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::event_header::Header;

/// Decoder for PARTIAL_UPDATE_ROWS_EVENT (MySQL 8.0+)
///
/// # 行布局(已用真实事件逐字节验证)
///
/// `PARTIAL_UPDATE_ROWS_EVENT`(type 39)与 `UPDATE_ROWS_EVENT`(type 31)共用同一
/// post-header 与列位图，差异在**行内**:
///
/// ```text
/// row = before_image || value_options(lenenc) || [partial_bits] || after_image
/// ```
///
/// - `value_options`: lenenc 整数，目前只取 `0` 或 `1`，bit0 = `PARTIAL_JSON_UPDATES`。
/// - `partial_bits`: 仅当 bit0 置位时存在，宽度 = `(表中 JSON 列数 + 7) / 8` 字节，
///   无长度前缀，LSB-first；bit i 对应第 i 个 **JSON** 列，
///   为 1 表示该列在 after-image 中以 **partial diff** 存储，为 0 表示完整值。
/// - JSON 列长度为 `0` 表示"本行未修改该列"，沿用 before-image 的值。
///
/// # 为什么不能"当作普通 UPDATE 解析"
///
/// 若按普通 UPDATE 解析，`value_options`/`partial_bits` 会顶替 after-image 的
/// null 位图与列值，解析器**不会报错**，而是产出看似正常但完全错误的行数据——
/// 这是 CDC 最危险的失败模式(静默数据损坏)。
///
/// 本解码器改为按上述布局正确解析(见 `parse_partial_update_rows_event`)。
/// 唯一仍不支持的是**真正的 partial JSON diff**(`partial_bits` 对应位为 1)：
/// 还原它需要实现 MySQL 的 `Json_diff` 二进制格式并与 before-image 合并，
/// 此时会显式报错，而不会把 diff 当作完整 JSON 文档返回。
#[derive(Debug)]
pub struct PartialUpdateRowsEventDecoder;

impl EventDecoder for PartialUpdateRowsEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::PARTIAL_UPDATE_ROWS_EVENT.as_u8() }
    fn name(&self) -> &'static str { "PartialUpdateRowsEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        context: LogContextRef,
        table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        use crate::events::protocol::partial_update_rows_event::parse_partial_update_rows_event;

        let event = parse_partial_update_rows_event(cursor, header, context, table_map)?;
        Ok(BinlogEvent::UpdateRows(event))
    }
}

/// Decoder for HEARTBEAT_LOG_EVENT_V2 (MySQL 8.0.26+)
/// Enhanced heartbeat event with additional metadata
#[derive(Debug)]
pub struct HeartbeatLogEventV2Decoder;

impl EventDecoder for HeartbeatLogEventV2Decoder {
    fn event_type(&self) -> u8 { LogEventType::HEARTBEAT_LOG_EVENT_V2.as_u8() }
    fn name(&self) -> &'static str { "HeartbeatLogEventV2Decoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::Normal }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let error_context = ErrorContext::new()
            .with_event_type(self.event_type())
            .with_position(header.borrow().get_log_pos())
            .with_operation("decode_heartbeat_v2".to_string());

        // Read the enhanced heartbeat data
        let remaining_data = cursor.get_ref().len() - cursor.position() as usize;
        
        if remaining_data < 4 {
            return Err(ReError::invalid_data_format_with_length(
                "HeartbeatLogEventV2 too short".to_string(),
                error_context,
                Some(4),
                Some(remaining_data)
            ));
        }

        // For now, we'll create a basic heartbeat event
        // In a full implementation, this would parse additional V2 metadata
        Ok(BinlogEvent::HEARTBEAT_LOG_V2)
    }
}

/// Decoder for TRANSACTION_CONTEXT_EVENT (MySQL 5.7+, enhanced in 8.0)
/// Contains transaction context information
#[derive(Debug)]
pub struct TransactionContextEventDecoder;

impl EventDecoder for TransactionContextEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::TRANSACTION_CONTEXT_EVENT.as_u8() }
    fn name(&self) -> &'static str { "TransactionContextEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::Normal }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let error_context = ErrorContext::new()
            .with_event_type(self.event_type())
            .with_position(header.borrow().get_log_pos())
            .with_operation("decode_transaction_context".to_string());

        // Read basic transaction context data
        let remaining_data = cursor.get_ref().len() - cursor.position() as usize;
        
        if remaining_data < 8 {
            return Err(ReError::invalid_data_format_with_length(
                "TransactionContextEvent too short".to_string(),
                error_context,
                Some(8),
                Some(remaining_data)
            ));
        }

        // For now, return a placeholder
        // In a full implementation, this would parse:
        // - Server UUID
        // - Thread ID
        // - Sequence number
        // - Other transaction metadata
        
        Ok(BinlogEvent::TRANSACTION_CONTEXT)
    }
}

/// Decoder for VIEW_CHANGE_EVENT (MySQL Group Replication)
/// Used in MySQL Group Replication for view changes
#[derive(Debug)]
pub struct ViewChangeEventDecoder;

impl EventDecoder for ViewChangeEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::VIEW_CHANGE_EVENT.as_u8() }
    fn name(&self) -> &'static str { "ViewChangeEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::Normal }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let error_context = ErrorContext::new()
            .with_event_type(self.event_type())
            .with_position(header.borrow().get_log_pos())
            .with_operation("decode_view_change".to_string());

        // Read view change data
        let remaining_data = cursor.get_ref().len() - cursor.position() as usize;
        
        if remaining_data < 16 {
            return Err(ReError::invalid_data_format_with_length(
                "ViewChangeEvent too short".to_string(),
                error_context,
                Some(16),
                Some(remaining_data)
            ));
        }

        // For now, return a placeholder
        // In a full implementation, this would parse:
        // - View ID
        // - Certification info
        // - Member information
        
        Ok(BinlogEvent::VIEW_CHANGE)
    }
}

/// Decoder for XA_PREPARE_LOG_EVENT (MySQL 5.7+)
/// XA transaction prepare event
#[derive(Debug)]
pub struct XaPrepareLogEventDecoder;

impl EventDecoder for XaPrepareLogEventDecoder {
    fn event_type(&self) -> u8 { LogEventType::XA_PREPARE_LOG_EVENT.as_u8() }
    fn name(&self) -> &'static str { "XaPrepareLogEventDecoder" }
    fn priority(&self) -> DecoderPriority { DecoderPriority::High }

    fn decode(
        &self,
        cursor: &mut Cursor<&[u8]>,
        header: HeaderRef,
        _context: LogContextRef,
        _table_map: Option<&HashMap<u64, TableMapEvent>>,
        _table_cache_manager: Option<&TableCacheManager>,
    ) -> Result<BinlogEvent, ReError> {
        let error_context = ErrorContext::new()
            .with_event_type(self.event_type())
            .with_position(header.borrow().get_log_pos())
            .with_operation("decode_xa_prepare".to_string());

        // Read XA prepare data
        let remaining_data = cursor.get_ref().len() - cursor.position() as usize;
        
        if remaining_data < 8 {
            return Err(ReError::invalid_data_format_with_length(
                "XaPrepareLogEvent too short".to_string(),
                error_context,
                Some(8),
                Some(remaining_data)
            ));
        }

        // 按真实布局解析 XID: one_phase(1) + format_id(4) + gtrid_len(4) + bqual_len(4) + gtrid + bqual
        
        use crate::events::protocol::xa_prepare_log_event::parse_xa_prepare_log_event;

        let event = parse_xa_prepare_log_event(cursor).map_err(|e| {
            ReError::event_parse_error_with_source(
                "Failed to parse XA_PREPARE_LOG_EVENT".to_string(),
                error_context,
                e,
            )
        })?;

        Ok(BinlogEvent::XA_PREPARE_LOG(event))
    }
}

/// Function to register MySQL 8.0 specific decoders
pub fn register_mysql8_decoders(registry: &crate::decoder::event_decoder_registry::EventDecoderRegistry) -> Result<(), ReError> {
    // Register MySQL 8.0+ specific event decoders
    //
    // 注意: TRANSACTION_PAYLOAD_EVENT(压缩事务)不在此注册。
    // 它是"一个网络事件 → 多个逻辑事件"，无法用返回单个事件的 EventDecoder 接口表达，
    // 因此由 `LogEventDecoder::event_parse` 直接展开(见 `transaction_payload` 模块)。
    registry.register_decoder(Box::new(PartialUpdateRowsEventDecoder))?;
    registry.register_decoder(Box::new(HeartbeatLogEventV2Decoder))?;
    registry.register_decoder(Box::new(TransactionContextEventDecoder))?;
    registry.register_decoder(Box::new(ViewChangeEventDecoder))?;
    registry.register_decoder(Box::new(XaPrepareLogEventDecoder))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::event_decoder_registry::EventDecoderRegistry;

    #[test]
    fn test_mysql8_decoder_registration() {
        let registry = EventDecoderRegistry::new();
        
        // Test registration of MySQL 8.0 decoders
        assert!(register_mysql8_decoders(&registry).is_ok());
        
        // Verify that the decoders are registered
        //
        // TRANSACTION_PAYLOAD_EVENT 由 LogEventDecoder::event_parse 特殊展开(含解压)，
        // 不注册单事件解码器，否则会被误判为"只产出一个事件"。
        assert!(!registry.has_decoder(LogEventType::TRANSACTION_PAYLOAD_EVENT.as_u8()));
        assert!(registry.has_decoder(LogEventType::PARTIAL_UPDATE_ROWS_EVENT.as_u8()));
        assert!(registry.has_decoder(LogEventType::HEARTBEAT_LOG_EVENT_V2.as_u8()));
        assert!(registry.has_decoder(LogEventType::TRANSACTION_CONTEXT_EVENT.as_u8()));
        assert!(registry.has_decoder(LogEventType::VIEW_CHANGE_EVENT.as_u8()));
        assert!(registry.has_decoder(LogEventType::XA_PREPARE_LOG_EVENT.as_u8()));
    }

    #[test]
    fn test_decoder_priorities() {
        let partial_update_decoder = PartialUpdateRowsEventDecoder;
        let xa_prepare_decoder = XaPrepareLogEventDecoder;
        
        // Verify that critical decoders have high priority
        assert_eq!(partial_update_decoder.priority(), DecoderPriority::High);
        assert_eq!(xa_prepare_decoder.priority(), DecoderPriority::High);
    }

    /// 部分更新事件走的是专用解析路径(不是 `UpdateRowsEvent::parse`)，
    /// 缺少前置 `Table_map` 时必须报"找不到 table map"，而不是静默产出空行。
    #[test]
    fn test_partial_update_rows_requires_table_map() {
        use std::cell::RefCell;
        use std::rc::Rc;
        use crate::events::log_context::LogContext;

        let decoder = PartialUpdateRowsEventDecoder;

        // 最小合法结构: table_id(6) flags(2) extra_data_len(2) cols(1) before(1) after(1) + 行数据 + 校验和
        let body: Vec<u8> = vec![
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // table_id = 1
            0x01, 0x00, // flags
            0x02, 0x00, // extra_data_len
            0x05, // columns_number = 5
            0xff, // before image
            0xff, // after image
            0x00, 0x00, 0x00, 0x00, // 行数据(占位)
            0x00, 0x00, 0x00, 0x00, // 校验和
        ];

        let header = Rc::new(RefCell::new(Header::new(
            "binlog.000002".to_string(),
            0,
            LogEventType::PARTIAL_UPDATE_ROWS_EVENT.as_u8(),
            1,
            (body.len() + 19) as u32,
            1000,
            1,
        )));
        let context = Rc::new(RefCell::new(LogContext::default()));

        let err = decoder
            .decode(&mut Cursor::new(body.as_slice()), header, context, None, None)
            .expect_err("without a preceding Table_map the row event must fail loudly");

        let message = format!("{}", err);
        assert!(
            message.contains("TableMapEvent") || message.contains("table map"),
            "expected a missing-table-map error, got: {}",
            message
        );
    }
}