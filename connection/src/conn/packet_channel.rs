use std::fs::File;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::os::fd::AsRawFd;
use std::{fmt, io, net};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use native_tls::{Certificate, TlsConnector};

use common::err::decode_error::ReError;
use common::err::CResult;

use crate::conn::connection_options::ConnectionOptions;
use crate::{PACKET_HEADER_SIZE, TIMEOUT_LATENCY_DELTA};

#[derive(Debug)]
pub struct PacketChannel {
    stream: ChannelStream,
}

impl PacketChannel {
    pub fn new(options: &ConnectionOptions) -> CResult<Self> {
        let address: String = format!("{}:{}", options.hostname, options.port.to_string());
        let stream = TcpStream::connect(address)?;
        let read_timeout = options.heartbeat_interval + TIMEOUT_LATENCY_DELTA;
        stream.set_read_timeout(Some(read_timeout))?;
        Ok(Self {
            stream: ChannelStream::Tcp(stream),
        })
    }

    pub fn is_ssl(&self) -> bool {
        match self.stream {
            ChannelStream::Tls(_) => true,
            _ => false,
        }
    }

    pub fn read_packet(&mut self) -> CResult<(Vec<u8>, u8)> {
        let mut header_buffer = [0; PACKET_HEADER_SIZE];

        self.stream.read_exact(&mut header_buffer)?;
        let packet_size = (&header_buffer[0..3]).read_u24::<LittleEndian>()?;
        let seq_num = header_buffer[3];

        let mut packet: Vec<u8> = vec![0; packet_size as usize];
        self.stream.read_exact(&mut packet)?;

        Ok((packet, seq_num))
    }

    pub fn write_packet(&mut self, packet: &[u8], seq_num: u8) -> CResult<()> {
        let packet_len = packet.len() as u32;
        self.stream.write_u24::<LittleEndian>(packet_len)?;
        self.stream.write_u8(seq_num)?;
        self.stream.write(packet)?;
        Ok(())
    }

    pub fn upgrade_to_ssl(self, options: &ConnectionOptions) -> CResult<Self> {
        if options.ssl_opts.is_none() {
            return Err(ReError::ConnectionError(
                "The ssl options is empty.".to_string(),
            ));
        }
        let ssl_opts = options.ssl_opts.clone().unwrap();

        let domain = options.hostname.clone();

        let mut builder = TlsConnector::builder();
        if let Some(root_cert_path) = ssl_opts.root_cert_path() {
            let mut root_cert_data = vec![];
            let mut root_cert_file = File::open(root_cert_path)?;
            root_cert_file.read_to_end(&mut root_cert_data)?;

            let root_certs = match Certificate::from_der(&root_cert_data)
                .map(|x| vec![x])
                .or_else(|_| {
                    pem::parse_many(&*root_cert_data)
                        .unwrap_or_default()
                        .iter()
                        .map(pem::encode)
                        .map(|s| Certificate::from_pem(s.as_bytes()))
                        .collect()
                }) {
                Ok(cert) => cert,
                Err(err) => {
                    return Err(ReError::ConnectionError(format!(
                        "The ssl cert can not load. err:{{{err}}}"
                    )))
                }
            };

            for root_cert in root_certs {
                builder.add_root_certificate(root_cert);
            }
        }
        if let Some(client_identity) = ssl_opts.client_identity() {
            let identity = client_identity.load()?;
            builder.identity(identity);
        }
        builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
        builder.danger_accept_invalid_certs(ssl_opts.accept_invalid_certs());
        let tls_connector = match builder.build() {
            Ok(tls) => tls,
            Err(err) => {
                return Err(ReError::ConnectionError(format!(
                    "Can not build tls. err:{{{err}}}"
                )))
            }
        };

        match self.stream {
            ChannelStream::Tcp(tcp_stream) => {
                let secure_stream = match tls_connector.connect(&domain, tcp_stream) {
                    Ok(stream) => stream,
                    Err(err) => {
                        return Err(ReError::ConnectionError(format!(
                            "Can not connect tls. err:{{{err}}}"
                        )))
                    }
                };
                Ok(Self {
                    stream: ChannelStream::Tls(secure_stream),
                })
            }
            ChannelStream::Tls(_) => Ok(self),
        }
    }
}

impl Default for PacketChannel {
    fn default() -> Self {
        PacketChannel::new(&ConnectionOptions::default()).unwrap()
    }
}

enum ChannelStream {
    Tls(native_tls::TlsStream<net::TcpStream>),
    Tcp(net::TcpStream),
}

impl ChannelStream {
    pub fn shutdown(&mut self) -> io::Result<()> {
        match self {
            ChannelStream::Tcp(stream) => stream.shutdown(Shutdown::Both),
            ChannelStream::Tls(stream) => stream.shutdown(),
        }
    }
}

impl Write for ChannelStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            ChannelStream::Tcp(stream) => stream.write(buf),
            ChannelStream::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            ChannelStream::Tcp(stream) => stream.flush(),
            ChannelStream::Tls(stream) => stream.flush(),
        }
    }
}

impl Read for ChannelStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            ChannelStream::Tcp(stream) => stream.read(buf),
            ChannelStream::Tls(stream) => stream.read(buf),
        }
    }
}

impl fmt::Debug for ChannelStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ChannelStream::Tcp(ref s) => write!(f, "Tcp stream {:?}", s),
            ChannelStream::Tls(ref s) => write!(f, "Tls stream {:?}", s),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}
