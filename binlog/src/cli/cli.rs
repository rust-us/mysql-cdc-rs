use std::fs::File;
use std::io::prelude::*;
use structopt::{clap::arg_enum, StructOpt};
use tokio::runtime::Runtime;
use tracing::{debug, error};
use common::log::tracing_factory::TracingFactory;
use crate::events::event::Event;
use crate::events::protocol::format_description_log_event::FormatDescriptionEvent;
use crate::factory::event_factory::{EventFactory, EventReaderOption, IEventFactory};

#[derive(Debug, StructOpt)]
#[structopt(name = "rustcdc-cli", about = "MySQL binlog tool impl with Rust")]
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

fn parse_from_file(path: &str, mut factory: EventFactory, options:EventReaderOption) -> Result<Vec<Event>, String> {
    match read_input(path) {
        Err(e) => Err(format!("failed to read {}: {}", path, e)),
        Ok((size, data)) => {
            debug!("read {} bytes", size);
            match factory.parser_bytes(&data, &options) {
                Err(e) => Err(format!("failed to parse binlog: {:?}", e)),
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

pub fn cli_start() {
    let args = Args::from_args();

    TracingFactory::init_log(args.debug);

    let factory = EventFactory::new(true);
    let options = EventReaderOption::default();
    match args.sub {
        Cmd::Trans {
            input,
            output,
            format,
        } => match parse_from_file(&input, factory, options) {
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
        Cmd::Desc { input } => match parse_from_file(&input, factory, options) {
            Err(e) => println!("{}", e),
            Ok(events) => {
                println!("Total: Events: {}", events.len());
                match events.first().unwrap() {
                    Event::FormatDescription(FormatDescriptionEvent {
                                                 binlog_version,
                                                 server_version,
                                                 create_timestamp,
                                                 ..
                                             })  => {
                        println!("Binlog version: {}", binlog_version);
                        println!("Server version: {}", server_version);
                        println!("Create_timestamp: {}", create_timestamp);
                    }
                    _ => unreachable!(),
                }
            }
        },
        Cmd::Conn { url, id } => {
            let mut rt = Runtime::new().expect("unable to launch runtime");
            rt.block_on(async {
                error!("failed to try connection mysql server, url:{}, id:{}.", url, id);
            })
        }
    }
}

#[cfg(test)]
mod test {
    use common::log::tracing_factory::TracingFactory;

    #[test]
    fn test() {
        TracingFactory::init_log(true);

        println!("test");
    }
}