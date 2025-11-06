use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tracing::{error, info, warn};
use common::err::decode_error::{Needed, ReError, ErrorContext};
use crate::b_type::LogEventType;
use crate::binlog_server::TABLE_MAP_EVENT;
use crate::decoder::event_decoder_registry::{EventDecoderRegistry, EventDecoderRegistryRef};
use crate::decoder::concrete_decoders::register_standard_decoders;
use crate::decoder::event_statistics::EventStatsCollector;
use crate::decoder::table_cache_manager::TableCacheManager;
use crate::events::checksum_type::ChecksumType;
use crate::events::binlog_event::BinlogEvent;
use crate::events::event_raw::{HeaderRef};
use crate::events::log_context::{ILogContext, LogContextRef};
use crate::events::log_position::LogFilePosition;
use crate::events::protocol::table_map_event::TableMapEvent;
use crate::events::declare::rows_log_event::RowsLogEvent;

// is EventParser
#[derive(Debug, Clone)]
pub struct LogEventDecoder {
    /// Gets checksum algorithm type used in a binlog file.
    pub checksum_type: ChecksumType,

    /// Gets TableMapEvent cache required in row events.
    pub table_map: HashMap<u64, TableMapEvent>,

    /// 上次未处理完的包
    remaing_bytes: Vec<u8>,

    table_cache_manager: TableCacheManager,

    /// Event decoder registry for dynamic event handling
    decoder_registry: EventDecoderRegistryRef,

    /// Statistics collector for monitoring and analysis
    stats_collector: EventStatsCollector,
}

impl LogEventDecoder {
    #[inline]
    pub fn new() -> Result<Self, ReError> {
        let registry = Arc::new(EventDecoderRegistry::new());
        
        // Register all standard decoders
        register_standard_decoders(&registry)?;

        Ok(Self {
            checksum_type: ChecksumType::None,
            table_map: HashMap::new(),
            remaing_bytes: Vec::new(),
            table_cache_manager: TableCacheManager::new(),
            decoder_registry: registry,
            stats_collector: EventStatsCollector::new(),
        })
    }

    /// Create a new decoder with a custom registry
    pub fn with_registry(registry: EventDecoderRegistryRef) -> Self {
        Self {
            checksum_type: ChecksumType::None,
            table_map: HashMap::new(),
            remaing_bytes: Vec::new(),
            table_cache_manager: TableCacheManager::new(),
            decoder_registry: registry,
            stats_collector: EventStatsCollector::new(),
        }
    }

    /// Create a new decoder with custom registry and stats collector
    pub fn with_registry_and_stats(registry: EventDecoderRegistryRef, stats_collector: EventStatsCollector) -> Self {
        Self {
            checksum_type: ChecksumType::None,
            table_map: HashMap::new(),
            remaing_bytes: Vec::new(),
            table_cache_manager: TableCacheManager::new(),
            decoder_registry: registry,
            stats_collector,
        }
    }

    /// Get a reference to the decoder registry
    pub fn get_registry(&self) -> &EventDecoderRegistryRef {
        &self.decoder_registry
    }

    /// Get a reference to the statistics collector
    pub fn get_stats_collector(&self) -> &EventStatsCollector {
        &self.stats_collector
    }

    /// Get current parsing statistics
    pub fn get_parse_stats(&self) -> crate::decoder::event_statistics::ParseStats {
        self.stats_collector.get_stats()
    }

    /// Get supported event types
    pub fn get_supported_event_types(&self) -> Vec<u8> {
        self.decoder_registry.get_supported_event_types()
    }

    /// Set the current binlog file being processed
    pub fn set_current_file(&self, filename: String) {
        self.stats_collector.set_current_file(filename);
    }

    /// Reset parsing statistics
    pub fn reset_stats(&self) {
        self.stats_collector.reset();
    }

    /// Export statistics as JSON
    pub fn export_stats_json(&self) -> Result<String, serde_json::Error> {
        self.stats_collector.export_json()
    }

    /// Get a summary report of parsing statistics
    pub fn get_stats_summary(&self) -> String {
        self.stats_collector.get_summary_report()
    }

    /// Parsing and processing of each event
    pub fn event_parse_mergr(&mut self, slice: &[u8], mut header: HeaderRef,
                       mut context: LogContextRef) -> Result<BinlogEvent, ReError> {
        // let mut parser_bytes = Vec::<u8>::new();
        // if self.remaing_bytes.len() > 0 {
        //     parser_bytes.extend(&self.remaing_bytes);
        //     self.remaing_bytes.clear();
        // }
        // parser_bytes.extend(slice);

        let result = self.event_parse(&slice, header.clone(), context);

        // match result.as_ref() {
        //     Ok(e) => {
        //         let event_len = header.borrow().event_length as usize;
        //         if slice.len() != event_len {
        //             let use_bytes = &slice[0..event_len];
        //             let rem_bytes = &slice[event_len..];
        //
        //             // append rem_bytes
        //             self.remaing_bytes.extend(rem_bytes);
        //         }
        //     }
        //     Err(err) => {}
        // };

        result
    }

    /// Parsing and processing of each event
    pub fn event_parse(&mut self, slice: &[u8], mut header: HeaderRef,
                                  mut context: LogContextRef) -> Result<BinlogEvent, ReError> {
        let checksum_type = &self.checksum_type;
        let position = header.borrow().get_log_pos();
        let b_type = header.borrow().event_type;
        
        // Create error context for this parsing operation
        let error_context = ErrorContext::new()
            .with_position(position)
            .with_event_type(b_type)
            .with_operation("event_parse".to_string());

        // Start timing the event parsing
        self.stats_collector.start_event_timer(position);

        // Consider verifying checksum
        let mut cursor = match checksum_type {
            ChecksumType::None => Cursor::new(slice.clone()),
            // 此处认为 slice 中不应该包含 crc信息。 暂时在内部处理掉， 后续再同意约束处理
            ChecksumType::Crc32 => Cursor::new(slice.clone()),
            // ChecksumType::Crc32 => Cursor::new(&slice.clone()[0..slice.len() - 4]),
        };

        let type_ = LogEventType::from(b_type);
        let has_gtid = context.borrow().get_gtid_set().is_some();
        
        // Try to decode using the registry first
        let binlog_event = if self.decoder_registry.has_decoder(b_type) {
            // Use the registry to decode the event
            let mut event = self.decoder_registry.decode_event(
                b_type,
                slice,
                header.clone(),
                context.clone(),
                Some(&self.table_map),
                Some(&self.table_cache_manager),
            )?;

            // Post-processing for specific event types
            match &mut event {
                BinlogEvent::Query(query_event) => {
                    if query_event.has_table_info() {
                        match query_event.get_table_info() {
                            Some(t) => { self.table_cache_manager.fresh_table_info(t); },
                            None => return Err(ReError::event_parse_error(
                                "QueryEvent has_table_info returned true but get_table_info returned None".to_string(),
                                error_context.clone()
                            )),
                        }
                    }
                    
                    header.borrow_mut().update_gtid(
                        context.borrow().get_gtid_set(),
                        context.borrow().get_gtid_log_event()
                    );
                },
                BinlogEvent::Rotate(rotate_event) => {
                    // updating new position in context
                    context.borrow_mut().force_set_log_position(
                        LogFilePosition::new_with_position(&rotate_event.get_file_name(), rotate_event.get_binlog_position())
                    );
                },
                BinlogEvent::FormatDescription(format_event) => {
                    context.borrow_mut().set_format_description(format_event.clone());
                },
                BinlogEvent::TableMap(table_event) => {
                    context.borrow_mut().put_table(table_event.table_id, table_event.clone());
                },
                BinlogEvent::WriteRows(write_event) => {
                    let table_id = write_event.table_id;
                    write_event.fill_assembly_table(context.clone())
                        .map_err(|e| ReError::event_parse_error_with_source(
                            "Failed to fill assembly table for WriteRowsEvent".to_string(),
                            error_context.clone().with_table_id(table_id),
                            e
                        ))?;
                    
                    header.borrow_mut().update_gtid(
                        context.borrow().get_gtid_set(),
                        context.borrow().get_gtid_log_event()
                    );
                },
                BinlogEvent::UpdateRows(update_event) => {
                    let table_id = update_event.table_id;
                    update_event.fill_assembly_table(context.clone())
                        .map_err(|e| ReError::event_parse_error_with_source(
                            "Failed to fill assembly table for UpdateRowsEvent".to_string(),
                            error_context.clone().with_table_id(table_id),
                            e
                        ))?;
                    
                    header.borrow_mut().update_gtid(
                        context.borrow().get_gtid_set(),
                        context.borrow().get_gtid_log_event()
                    );
                },
                BinlogEvent::DeleteRows(delete_event) => {
                    let table_id = delete_event.table_id;
                    delete_event.fill_assembly_table(context.clone())
                        .map_err(|e| ReError::event_parse_error_with_source(
                            "Failed to fill assembly table for DeleteRowsEvent".to_string(),
                            error_context.clone().with_table_id(table_id),
                            e
                        ))?;
                    
                    header.borrow_mut().update_gtid(
                        context.borrow().get_gtid_set(),
                        context.borrow().get_gtid_log_event()
                    );
                },
                BinlogEvent::GtidLog(gtid_event) => {
                    if has_gtid {
                        context.borrow_mut().update_gtid_set(gtid_event.get_gtid_str());

                        // update latest gtid
                        if let Some(gtid_set) = context.borrow().get_gtid_set() {
                            header.borrow_mut().update_gtid(
                                Some(gtid_set),
                                context.borrow().get_gtid_log_event()
                            );
                        }
                    }

                    // update current gtid event to context
                    context.borrow_mut().set_gtid_log_event(gtid_event.clone());
                },
                BinlogEvent::AnonymousGtidLog(gtid_event) => {
                    if has_gtid {
                        context.borrow_mut().update_gtid_set(gtid_event.get_gtid_str());

                        // update latest gtid
                        if let Some(gtid_set) = context.borrow().get_gtid_set() {
                            header.borrow_mut().update_gtid(
                                Some(gtid_set),
                                context.borrow().get_gtid_log_event()
                            );
                        }
                    }

                    context.borrow_mut().set_gtid_log_event(gtid_event.clone());
                },
                BinlogEvent::XID(_) | BinlogEvent::UserVar(_) => {
                    header.borrow_mut().update_gtid(
                        context.borrow().get_gtid_set(),
                        context.borrow().get_gtid_log_event()
                    );
                },
                _ => {
                    // No special post-processing needed for other events
                }
            }

            // Update position for all events
            context.borrow_mut().update_position_offset(header.borrow().get_log_pos());

            Ok(event)
        } else {
            // Fallback to hardcoded handling for unsupported events
            match type_ {
                // Handle PRE_GA events that are not in the registry
                LogEventType::PRE_GA_WRITE_ROWS_EVENT |
                LogEventType::PRE_GA_UPDATE_ROWS_EVENT |
                LogEventType::PRE_GA_DELETE_ROWS_EVENT => {
                    warn!("Skipping unsupported PRE_GA event from position: {}", header.borrow().get_log_pos());
                    context.borrow_mut().update_position_offset(header.borrow().get_log_pos());
                    Ok(BinlogEvent::IgnorableLogEvent)
                },
                
                // Handle future MySQL 8.0+ events that might not be implemented yet
                _ => {
                    let code = type_.as_val();
                    error!("Unsupported event type: 0x{:02x}", code);
                    Err(ReError::unsupported_event_type(
                        code as u8,
                        error_context.clone().with_info(format!("Event type 0x{:02x} not supported by any registered decoder", code))
                    ))
                }
            }
        };
        //
        // // check
        // info!("{}", format!("event_size: {}. {}/{}", event_size, context.borrow().get_log_file_position(), context.borrow().get_global_position()));

        match binlog_event {
            Ok(e) => {
                // Record successful parsing
                self.stats_collector.record_event_success(&e, position);

                if let BinlogEvent::FormatDescription(x) = &e {
                    self.checksum_type = x.get_checksum_type();
                }

                if let BinlogEvent::TableMap(table_event) = &e {
                    //todo: optimize
                    self.table_map.insert(table_event.table_id, table_event.clone());
                    // 兼容
                    TABLE_MAP_EVENT.lock().unwrap().insert(table_event.table_id, table_event.clone());
                }

                return Ok(e);
            },
            Err(err) => {
                // Record parsing error
                self.stats_collector.record_event_error(b_type, position);
                Err(err)
            }
        }
    }
}
