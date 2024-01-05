use std::net::Shutdown;
use std::os::{
    unix,
    unix::io::{AsRawFd, RawFd},
};
use std::{
    fmt, io,
    net::{self, SocketAddr},
    time::Duration,
};

use bufstream::BufStream;
use io_enum::*;

use crate::mysql::error::{
    DriverError::{ConnectTimeout, CouldNotConnect},
    Error::DriverError,
    Error::IoError,
    Result as MyResult,
};

mod tcp;
mod tls;

#[derive(Debug, Read, Write)]
pub enum Stream {
    SocketStream(BufStream<unix::net::UnixStream>),
    TcpStream(TcpStream),
}

impl Stream {
    pub fn connect_socket(
        socket: &str,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> MyResult<Stream> {
        match unix::net::UnixStream::connect(socket) {
            Ok(stream) => {
                stream.set_read_timeout(read_timeout)?;
                stream.set_write_timeout(write_timeout)?;
                Ok(Stream::SocketStream(BufStream::new(stream)))
            }
            Err(e) => {
                let addr = socket.to_string();
                let desc = e.to_string();
                Err(DriverError(CouldNotConnect(Some((addr, desc, e.kind())))))
            }
        }
    }

    pub fn connect_tcp(
        ip_or_hostname: &str,
        port: u16,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
        tcp_keepalive_time: Option<u32>,
        tcp_keepalive_probe_interval_secs: Option<u32>,
        tcp_keepalive_probe_count: Option<u32>,
        tcp_user_timeout: Option<u32>,
        nodelay: bool,
        tcp_connect_timeout: Option<Duration>,
        bind_address: Option<SocketAddr>,
    ) -> MyResult<Stream> {
        let mut builder = tcp::MyTcpBuilder::new((ip_or_hostname, port));
        builder
            .connect_timeout(tcp_connect_timeout)
            .read_timeout(read_timeout)
            .write_timeout(write_timeout)
            .keepalive_time_ms(tcp_keepalive_time)
            .nodelay(nodelay)
            .bind_address(bind_address);
        //#[cfg(any(target_os = "linux", target_os = "macos",))]
        builder.keepalive_probe_interval_secs(tcp_keepalive_probe_interval_secs);
        //#[cfg(any(target_os = "linux", target_os = "macos",))]
        builder.keepalive_probe_count(tcp_keepalive_probe_count);
        //#[cfg(target_os = "linux")]
        builder.user_timeout(tcp_user_timeout);
        builder
            .connect()
            .map(|stream| Stream::TcpStream(TcpStream::Insecure(BufStream::new(stream))))
            .map_err(|err| {
                if err.kind() == io::ErrorKind::TimedOut {
                    DriverError(ConnectTimeout)
                } else {
                    let addr = format!("{}:{}", ip_or_hostname, port);
                    let desc = format!("{}", err);
                    DriverError(CouldNotConnect(Some((addr, desc, err.kind()))))
                }
            })
    }

    pub fn is_insecure(&self) -> bool {
        matches!(self, Stream::TcpStream(TcpStream::Insecure(_)))
    }

    pub fn is_socket(&self) -> bool {
        matches!(self, Stream::SocketStream(_))
    }

    pub fn shutdown(&mut self) -> MyResult<()> {
        match self {
            Stream::SocketStream(stream) => match stream.get_mut().shutdown(Shutdown::Both) {
                Ok(_) => Ok(()),
                Err(e) => Err(IoError(e)),
            },
            Stream::TcpStream(stream) => stream.shutdown(),
        }
    }
}

impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Stream::SocketStream(stream) => stream.get_ref().as_raw_fd(),
            Stream::TcpStream(stream) => stream.as_raw_fd(),
        }
    }
}

#[derive(Read, Write)]
pub enum TcpStream {
    Secure(BufStream<native_tls::TlsStream<net::TcpStream>>),
    Insecure(BufStream<net::TcpStream>),
}

impl TcpStream {
    pub fn shutdown(&mut self) -> MyResult<()> {
        let result = match self {
            TcpStream::Secure(stream) => stream.get_mut().shutdown(),
            TcpStream::Insecure(stream) => stream.get_mut().shutdown(Shutdown::Both),
        };
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(IoError(e)),
        }
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            TcpStream::Secure(stream) => stream.get_ref().get_ref().as_raw_fd(),
            TcpStream::Insecure(stream) => stream.get_ref().as_raw_fd(),
        }
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            TcpStream::Secure(ref s) => write!(f, "Secure stream {:?}", s),
            TcpStream::Insecure(ref s) => write!(f, "Insecure stream {:?}", s),
        }
    }
}
