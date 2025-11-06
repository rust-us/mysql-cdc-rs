use std::io;
use tracing::instrument::WithSubscriber;
use tracing::Level;
use tracing_appender::rolling;
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, Registry,
};
use tracing_subscriber::fmt::writer::MakeWriterExt;

/// TracingFactory 是否全局初始化完成
static mut is_init: bool = false;

#[derive(Debug, Clone, Default)]
pub struct TracingFactory {
    options: TracingFactoryOptions
}

#[derive(Debug, Clone)]
pub struct TracingFactoryOptions {
    debug: bool,

    output_type: OutputType,

    level: Option<Level>,

    log_dir: Option<String>,
}

#[derive(Debug, Clone)]
pub enum OutputType {
    STDOUT,

    LOG,
}

impl TracingFactory {
    pub fn init_log(debug: bool) -> Self {
        TracingFactory::init_log_with_options(TracingFactoryOptions::new_with_debug(debug))
    }

    pub fn init_log_with_options(opt: TracingFactoryOptions) -> Self {
        let mut opts = opt.clone();

        let dir = match opt.log_dir {
            None => {
                let path = String::from("/tmp/replayer/logs");
                opts.log_dir = Some(path.clone());

                path
            }
            Some(dir) => {dir.clone()}
        };

        let level  = match opts.level {
            None => {
                Level::INFO
            }
            Some(l) => {
                l
            }
        };

        unsafe {
            if !is_init {
                // Configure a custom event formatter
                let format = fmt::format()
                    .pretty()
                    // display source code file paths
                    .with_file(true)
                    // display source code line numbers
                    .with_line_number(false)
                    // .with_level(false) // don't include levels in formatted output
                    .with_target(false) // don't include targets, disable targets
                    // enable thread id to be emitted
                    .with_thread_ids(true) // include the thread ID of the current thread
                    // enabled thread name to be emitted
                    .with_thread_names(true) // include the name of the current thread
                    .compact(); // use the `Compact` formatting style.

                match opts.output_type {
                    OutputType::STDOUT => {
                        // let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout);

                        let _ = tracing_subscriber::fmt()
                            .with_max_level(level)
                            .event_format(format)
                            .pretty()
                            // .with_writer(non_blocking)
                            // sets this to be the default, global collector for this application.
                            .try_init();
                    },
                    OutputType::LOG => {
                        // debug 模式下，std 与 log 同时输出。 否则只输出 file
                        let file_appender = rolling::daily(format!("{}/binlog", dir.as_str()), "file.log");

                        let merge = file_appender.and(io::stdout);

                        let _ = tracing_subscriber::fmt()
                            .with_max_level(level)
                            .event_format(format)
                            .pretty()
                            .with_writer(merge)
                            // sets this to be the default, global collector for this application.
                            .try_init();
                    }
                };

                is_init = true;
            }
        }

        TracingFactory {
            options: opts.clone(),
        }
    }

    pub fn get_log_dir(&self) -> &str {
        self.options.get_log_dir()
    }
}

impl Default for TracingFactoryOptions {
    fn default() -> Self {
        TracingFactoryOptions::new_with_debug(true)
    }
}

impl TracingFactoryOptions {
    pub fn new_with_debug(debug: bool) -> Self {
        TracingFactoryOptions::new_with_type(debug, OutputType::STDOUT)
    }

    pub fn new_with_type(debug: bool, output_type: OutputType) -> Self {
        TracingFactoryOptions::new(debug, output_type, None)
    }

    pub fn new(debug: bool, output_type: OutputType, log_dir: Option<String>) -> Self {
        let level = if debug {
            Level::DEBUG
        } else {
            Level::INFO
        };

        TracingFactoryOptions {
            debug,
            output_type,
            level: Some(level),
            log_dir,
        }
    }

    pub fn get_log_dir(&self) -> &str {
        match self.log_dir.as_ref() {
            None => {""}
            Some(dir) => {dir.as_str()}
        }
    }
}

#[cfg(test)]
mod test {
    use tracing::{debug, error, info, warn};
    use crate::log::tracing_factory::{is_init, TracingFactory};

    #[test]
    fn test() {
        unsafe { assert!(!is_init); }
        TracingFactory::init_log(true);
        unsafe { assert!(is_init); }
        TracingFactory::init_log(true);
        unsafe { assert!(is_init); }

        debug!("TracingFactory test: {:?}", "test");
        info!("TracingFactory test: {:?}", "test");
        warn!("TracingFactory test: {:?}", "test");
        error!("TracingFactory test: {:?}", "test");
    }

}