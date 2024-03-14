use tracing::info;
use common::log::tracing_factory::TracingFactory;
use relay_log::codec::binary_codec::BinaryCodec;
use relay_log::codec::binary_codec::CodecStyle::LittleVar;
use relay_log::codec::codec::Codec;
use relay_log::relay_log::RelayLog;

#[test]
fn test_binary_codec() {
    TracingFactory::init_log(true);

    let mut s = RelayLog::default();
    s.set_database_name("db1".to_string());
    s.set_table_name("t2".to_string());
    s.set_event_log_pos(10);
    s.set_event_name("binlog".to_string());

    let codec = BinaryCodec::new();

    let bytes = codec.binary_serialize(&LittleVar, &s).unwrap();
    info!("序列化：{}-{:?}", bytes.len(), bytes);

    let s2 = codec.binary_deserialize::<RelayLog>(&LittleVar, &bytes).unwrap();
    info!("反序列化：{:?}", s2);
}