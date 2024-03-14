use tokio::join;
use tracing::info;

use common::log::tracing_factory::TracingFactory;
use common::schema::rc_task::RcTask;
use relay_log::relay_log_server_machine::RelayLogServerMachine;

#[tokio::test]
pub async fn test_relay_log_server_machine() {
    TracingFactory::init_log(true);

    let h1 = tokio::task::spawn_blocking(|| {
        add_task("1")
    });

    let h2 = tokio::task::spawn_blocking(|| {
        add_task("2")
    });

    let h3 = tokio::task::spawn_blocking(|| {
        add_task("3")
    });

    join!(h1, h2, h3);

    info!("{:?}", RelayLogServerMachine::get_instance());
}

fn add_task(id: &str) {
    let task = RcTask {
        task_id: id.to_string(),
        task_name: "".to_string(),
        src_info: vec![],
        dst_db_name: "".to_string(),
        dst_table_name: "".to_string(),
    };
    RelayLogServerMachine::get_instance().add_task(task).unwrap();
}