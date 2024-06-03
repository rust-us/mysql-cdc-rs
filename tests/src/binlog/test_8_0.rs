
#[cfg(test)]
mod test_normal {
    use std::fs::{File, OpenOptions};
    use std::path::Path;
    use tracing::debug;
    use common::binlog::column::column_value::SrcColumnValue::{BigInt, Int, MediumInt, SmallInt, TinyInt};
    use binlog::decoder::binlog_decoder::BinlogReader;
    use binlog::decoder::file_binlog_reader::FileBinlogReader;
    use binlog::events::binlog_event::BinlogEvent;
    use binlog::events::binlog_event::BinlogEvent::{DeleteRows, Query, TableMap, UpdateRows, WriteRows};
    use binlog::factory::event_factory::{EventFactory, EventReaderOption, IEventFactory};
    use binlog::events::protocol::delete_rows_v12_event::DeleteRowsEvent;
    use binlog::events::protocol::query_event::QueryEvent;
    use binlog::events::protocol::table_map_event::TableMapEvent;
    use binlog::events::protocol::update_rows_v12_event::UpdateRowsEvent;
    use binlog::events::protocol::write_rows_v12_event::WriteRowsEvent;
    use binlog::row::row_data::{RowData, UpdateRowData};
    use common::log::tracing_factory::TracingFactory;

    #[test]
    fn test() {
        TracingFactory::init_log(true);

        debug!("test");
    }

    #[test]
    fn test_table_map_event_write_rows_log_event() {
        let input = include_bytes!("../../events/8.0/19_30_Table_map_event_Write_rows_log_event/binlog.000018");
        let mut factory = EventFactory::new(false);
        let (remain, output) = factory.parser_bytes(input, &EventReaderOption::default()).unwrap();

        match output.get(8).unwrap() {
            TableMap(TableMapEvent{
                         table_id,
                         flags,
                         table_name,
                         column_metadata,
                      ..
                  })  => {
                assert_eq!(*table_id, 90);
                assert_eq!(*flags, 1);
                assert_eq!(*table_name, "int_table");
                assert_eq!(*column_metadata, vec![0, 0, 0, 0, 0, 0]);
            }
            _ => panic!("should TableMapEvent"),
        }

        match output.get(9).unwrap() {
            WriteRows(WriteRowsEvent{
                          table_id,
                          columns_number,
                          columns_present,
                          rows,
                      ..
                  })  => {
                assert_eq!(*table_id, 90);
                assert_eq!(*columns_number, 6);
            }
            _ => panic!("should WriteRowsEvent"),
        }
    }

    #[test]
    fn test_update_rows_log_event() {
        let input = include_bytes!("../../events/8.0/31_update_rows_v2/binlog.000001");
        let mut factory = EventFactory::new(false);
        let (remain, output) = factory.parser_bytes(input, &EventReaderOption::default()).unwrap();

        // values
        let before_update: RowData = RowData {
            cells: vec![
                Some(TinyInt(1)),
                Some(SmallInt(11)),
                Some(MediumInt(111)),
                Some(Int(1111)),
                Some(BigInt(11111)),
                Some(TinyInt(1)),
            ],
        };
        let after_update: RowData = RowData {
            cells: vec![
                Some(TinyInt(1)),
                Some(SmallInt(22)),
                Some(MediumInt(222)),
                Some(Int(1111)),
                Some(BigInt(11111)),
                Some(TinyInt(1)),
            ],
        };
        let row = UpdateRowData::new(before_update, after_update);
        let values = vec![
            row
        ];

        match output.get(13).unwrap() {
            TableMap(TableMapEvent{
                         table_id,
                         flags,
                         table_name,
                         column_metadata,
                      ..
                  })  => {
                assert_eq!(*table_id, 91);
                assert_eq!(*flags, 1);
                assert_eq!(*table_name, "int_table");
                assert_eq!(*column_metadata, vec![0, 0, 0, 0, 0, 0]);
            }
            _ => panic!("should TableMapEvent"),
        }

        match output.get(14).unwrap() {
            UpdateRows(UpdateRowsEvent{
                          table_id,
                          columns_number,
                          before_image_bits,
                          after_image_bits,
                          rows,
                      ..
                  })  => {
                assert_eq!(*table_id, 91);
                assert_eq!(*columns_number, 6);
                assert_eq!(*before_image_bits, vec![true, true, true, true, true, true]);
                assert_eq!(*after_image_bits, vec![true, true, true, true, true, true]);

                let rows_ = rows.clone();
                let len = &values.len();
                for i in 0..*len {
                    assert_eq!(rows_.get(i).unwrap(), values.get(i).unwrap());
                }
            }
            _ => panic!("should UpdateRowsEvent"),
        }
    }

    #[test]
    fn test_delete_rows_log_event() {
        let input = include_bytes!("../../events/8.0/32_delete_rows_v2/binlog.000001");
        let mut factory = EventFactory::new(false);
        let (remain, output) = factory.parser_bytes(input, &EventReaderOption::default()).unwrap();

        // values
        let delete: RowData = RowData {
            cells: vec![
                Some(TinyInt(1)),
                Some(SmallInt(22)),
                Some(MediumInt(222)),
                Some(Int(1111)),
                Some(BigInt(11111)),
                Some(TinyInt(1)),
            ],
        };
        let delete_val = vec![
            delete
        ];

        match output.get(18).unwrap() {
            TableMap(TableMapEvent{
                         table_id,
                         flags,
                         table_name,
                         column_metadata,
                      ..
                  })  => {
                assert_eq!(*table_id, 91);
                assert_eq!(*flags, 1);
                assert_eq!(*table_name, "int_table");
                assert_eq!(*column_metadata, vec![0, 0, 0, 0, 0, 0]);
            }
            _ => panic!("should TableMapEvent"),
        }

        match output.get(19).unwrap() {
            DeleteRows(DeleteRowsEvent{
                          table_id,
                          columns_number,
                           flags,
                           deleted_image_bits,
                          rows,
                      ..
                  })  => {
                assert_eq!(*table_id, 91);
                assert_eq!(*columns_number, 6);
                assert_eq!(*deleted_image_bits, vec![true, true, true, true, true, true]);

                let rows_ = rows.clone();
                let len = &delete_val.len();
                for i in 0..*len {
                    assert_eq!(rows_.get(i).unwrap(), delete_val.get(i).unwrap());
                }
            }
            _ => panic!("should DeleteRowsEvent"),
        }
    }

}