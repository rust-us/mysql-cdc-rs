use serde::{Serialize};
use common::config::load_style::Format;
use connection::binlog::binlog_subscribe::SubscribeOptions;

#[derive(Debug, Clone, Serialize)]
pub struct CliOptions {
    /// 是否调试模式
    debug: bool,

    /// 是否输出日志
    print_logs: bool,

    format: Format,

}

impl CliOptions {
    pub fn new(debug: bool, format: Format) -> Self {
        CliOptions {
            debug,
            print_logs: false,
            format,
        }
    }

    pub fn new_with_log(debug: bool, format: Format) -> Self {
        CliOptions {
            debug,
            print_logs: true,
            format,
        }
    }

    pub fn is_print_logs(&self) -> bool {
        self.print_logs
    }

    pub fn is_debug(&self) -> bool {
        self.debug
    }

    pub fn get_format(&self) -> Format {
        self.format.clone()
    }

    pub fn to_subscribe_options(&self) -> SubscribeOptions {
        SubscribeOptions::new(self.debug, self.print_logs, self.format.clone())
    }
}

impl Default for CliOptions {
    fn default() -> Self {
        CliOptions::new(false, Format::None)
    }
}