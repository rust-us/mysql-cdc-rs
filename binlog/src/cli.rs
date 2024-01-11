use crate::connection::Connection;
use crate::events::event::Event;

use std::fs::File;
use std::io::prelude::*;
use structopt::{clap::arg_enum, StructOpt};
use tokio::runtime::Runtime;
use common::log::log_factory::LogFactory;
use crate::events::event_factory::EventFactory;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;

#[derive(Debug, StructOpt)]
#[structopt(name = "binlog-cli", about = "MySQL binlog tool impl with Rust")]
pub struct Args {
    /// enable debug info
    #[structopt(short, long)]
    debug: bool,

    #[structopt(subcommand)]
    sub: Cmd,
}

#[derive(Debug, StructOpt)]
enum Cmd {
    /// Transform a binlog file to specified format
    Trans {
        /// Binlog file path
        input: String,

        /// Output file path, if not present, print to stdout
        output: Option<String>,

        /// Output format
        #[structopt(short, long, possible_values = &Format::variants(), case_insensitive = true, default_value = "Json")]
        format: Format,
    },

    /// Show bin log desc msg
    Desc {
        /// Binlog file path
        input: String,
    },

    /// Connect to a server
    Conn {
        /// Connection url
        url: String,

        /// client id
        id: u32,
    },
}

arg_enum! {
    #[derive(Debug)]
    enum Format {
        Json,
        Yaml,
    }
}

fn read_input(path: &str) -> std::io::Result<(usize, Vec<u8>)> {
    let mut f = File::open(path)?;
    let mut buf = vec![];
    let size = f.read_to_end(&mut buf)?;
    Ok((size, buf))
}

fn parse_from_file(path: &str) -> Result<Vec<Event>, String> {
    match read_input(path) {
        Err(e) => Err(format!("failed to read {}: {}", path, e)),
        Ok((size, data)) => {
            log::debug!("read {} bytes", size);
            match EventFactory::from_bytes(&data) {
                Err(e) => Err(format!("failed to parse binlog: {}", e)),
                Ok((remain, events)) => {
                    if remain.len() != 0 {
                        Err(format!("remain: {:?}", remain))
                    } else {
                        Ok(events)
                    }
                }
            }
        }
    }
}

fn main() {
    let args = Args::from_args();
    let _handle = LogFactory::init_log(args.debug);
    match args.sub {
        Cmd::Trans {
            input,
            output,
            format,
        } => match parse_from_file(&input) {
            Err(e) => {
                println!("{}", e);
            }
            Ok(events) => {
                if let Some(output) = output {
                    if let Ok(mut output) = File::create(output) {
                        match format {
                            Format::Json => {
                                output
                                    .write_all(
                                        serde_json::to_string_pretty(&events).unwrap().as_bytes(),
                                    )
                                    .unwrap();
                            }
                            Format::Yaml => {
                                output
                                    .write_all(serde_yaml::to_string(&events).unwrap().as_bytes())
                                    .unwrap();
                            }
                        }
                    }
                } else {
                    match format {
                        Format::Json => {
                            println!("{}", serde_json::to_string_pretty(&events).unwrap());
                        }
                        Format::Yaml => println!("{}", serde_yaml::to_string(&events).unwrap()),
                    }
                }
            }
        },
        Cmd::Desc { input } => match parse_from_file(&input) {
            Err(e) => println!("{}", e),
            Ok(events) => {
                println!("Total: Events: {}", events.len());
                match events.first().unwrap() {
                    Event::FormatDescription {
                        event: FormatDescriptionEvent {
                            binlog_version,
                            mysql_server_version,
                            create_timestamp,
                            ..
                        }
                    } => {
                        println!("Binlog version: {}", binlog_version);
                        println!("Server version: {}", mysql_server_version);
                        println!("Create_timestamp: {}", create_timestamp);
                    }
                    _ => unreachable!(),
                }
            }
        },
        Cmd::Conn { url, id } => {
            let mut rt = Runtime::new().expect("unable to launch runtime");
            rt.block_on(async {
                let mut conn = Connection::new(url, id);
                loop {
                    match conn.recv().await {
                        Ok(bytes) => match Event::parse(bytes.slice(1..).as_ref()) {
                            Ok((_, event)) => {
                                println!("\n{:#x?}\n", event);
                            }
                            Err(e) => log::error!(
                                "failed to parse packet: {:?} due to{}",
                                bytes.as_ref(),
                                e
                            ),
                        },
                        Err(e) => {
                            log::error!("failed to recv packet: {}", e);
                            break;
                        }
                    }
                }
            })
        }
    }
}

#[cfg(test)]
mod test_cli {
    use common::log::log_factory::LogFactory;

    #[test]
    fn test() {
        LogFactory::init_log(true);

        println!("cli test");
        log::debug!("cli test");
    }
}
