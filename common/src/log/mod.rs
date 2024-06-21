pub mod tracing_factory;


use tracing::Level;
use tracing_subscriber::FmtSubscriber;

// #[cfg(test)]
pub fn init_test_log() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("set default subscriber failed");
}

