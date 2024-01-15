use tracing::Level;
use tracing_subscriber::FmtSubscriber;

/// TracingFactory 是否全局初始化完成
static mut is_init: bool = false;

#[derive(Debug, Clone, Default)]
pub struct TracingFactory {

}

impl TracingFactory {
    pub fn init_log(debug: bool) {
        unsafe {
            if !is_init {
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(Level::TRACE)
                    .finish();
                tracing::subscriber::set_global_default(subscriber).expect("set default subscriber failed");

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