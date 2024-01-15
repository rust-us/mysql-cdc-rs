// use log::LevelFilter;
// use log4rs::{
//     append::console::{ConsoleAppender, Target},
//     config::{Appender, Config, Root},
//     Handle,
// };
// use std::io::prelude::*;
//
// #[derive(Debug, Clone, Default)]
// pub struct LogFactory {
//
// }
//
// impl LogFactory {
//     pub fn init_log(debug: bool) -> Handle {
//         let level = if debug {
//             LevelFilter::Debug
//         } else {
//             LevelFilter::Warn
//         };
//
//         let stdout = ConsoleAppender::builder().target(Target::Stdout).build();
//         let config = Config::builder()
//             .appender(Appender::builder().build("stdout", Box::new(stdout)))
//             .build(Root::builder().appender("stdout").build(level))
//             .unwrap();
//
//         log4rs::init_config(config).unwrap()
//     }
// }
// #[cfg(test)]
// mod test {
//     use log::log;
//     use crate::log::log_factory::LogFactory;
//
//     #[test]
//     fn test() {
//         LogFactory::init_log(true);
//
//         println!("LogFactory test");
//     }
//
// }