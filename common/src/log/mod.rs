pub mod tracing_factory;


use tracing::Level;
use tracing_subscriber::FmtSubscriber;

// #[cfg(test)]
pub fn init_test_log() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    
    // Ignore the error if a global subscriber has already been set
    let _ = tracing::subscriber::set_global_default(subscriber);
}

