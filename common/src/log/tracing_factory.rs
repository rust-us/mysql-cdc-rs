use tracing::Level;
use tracing_subscriber::{fmt, FmtSubscriber};

/// TracingFactory 是否全局初始化完成
static mut is_init: bool = false;

#[derive(Debug, Clone, Default)]
pub struct TracingFactory {

}

impl TracingFactory {
    pub fn init_log(debug: bool) {
        unsafe {
            if !is_init {
                // Configure a custom event formatter
                let format = fmt::format()
                    // .with_level(false) // don't include levels in formatted output
                    // .with_target(false) // don't include targets
                    .with_thread_ids(true) // include the thread ID of the current thread
                    // .with_thread_names(true) // include the name of the current thread
                    .compact(); // use the `Compact` formatting style.

                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::DEBUG)
                    .event_format(format)
                    .pretty()
                    .finish();

                tracing::subscriber::set_global_default(subscriber)
                    .map_err(|_err| eprintln!("Unable to set global default subscriber"))
                    .expect("set default subscriber failed");

                is_init = true;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tracing::debug;
    use crate::log::tracing_factory::{is_init, TracingFactory};

    #[test]
    fn test() {
        unsafe { assert!(!is_init); }
        TracingFactory::init_log(true);
        unsafe { assert!(is_init); }
        TracingFactory::init_log(true);
        unsafe { assert!(is_init); }

        debug!("TracingFactory test: {:?}", "test");;
    }

}