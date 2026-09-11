use std::fs::File;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::{fmt, io, net};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use native_tls::{Certificate, TlsConnector};

use common::err::decode_error::ReError;
use common::err::CResult;

use crate::conn::connection_options::{ConnectionOptions, SslOpts};
use crate::{MAX_BODY_LENGTH, PACKET_HEADER_SIZE, TIMEOUT_LATENCY_DELTA};

pub struct PacketChannel {
    stream: ChannelStream,

    /// 已从 socket 读取、但尚未组成完整 MySQL 包的字节。
    ///
    /// 读超时可能发生在"包中途"(例如只读到半个包头)，
    /// 保留这些字节才能保证超时后重试不会丢数据、不会串包。
    read_buffer: Vec<u8>,

    /// 正在拼接的大包(>16MB 的协议分片)。
    ///
    /// MySQL 协议规定: 包体长度达到 0xFFFFFF 时表示后面还有后续分片，
    /// 需要拼接直到出现长度 < 0xFFFFFF 的分片(若总长恰为整数倍，末包长度为 0)。
    /// 该状态同样必须在读超时后保留，否则大包会丢一半。
    assembling: Option<AssemblingPacket>,
}

/// 正在拼接中的多分片大包
#[derive(Debug)]
struct AssemblingPacket {
    /// 已累计的载荷
    payload: Vec<u8>,
    /// 最近一个分片的序号
    seq_num: u8,
    /// 最近一个分片的长度(== MAX_BODY_LENGTH 表示还需要后续分片)
    last_fragment_len: usize,
}

impl PacketChannel {
    pub fn new(options: &ConnectionOptions) -> CResult<Self> {
        let address: String = format!("{}:{}", options.hostname, options.port.to_string());
        let stream = TcpStream::connect(address)?;
        // 显式设置了读超时则使用之；否则按"心跳间隔 + 网络延迟冗余"推导
        let read_timeout = options
            .read_timeout
            .unwrap_or(options.heartbeat_interval + TIMEOUT_LATENCY_DELTA);
        stream.set_read_timeout(Some(read_timeout))?;
        Ok(Self {
            stream: ChannelStream::Tcp(stream),
            read_buffer: Vec::new(),
            assembling: None,
        })
    }

    pub fn is_ssl(&self) -> bool {
        match self.stream {
            ChannelStream::Tls(_) => true,
            _ => false,
        }
    }

    /// 读取一个完整的 MySQL 数据包(含 >16MB 多分片重组)。
    ///
    /// 内部维护读取缓冲区：数据不足时继续从 socket 读取，
    /// 因此**读超时不会丢失已读字节**，调用方可以安全地重试
    /// (binlog 流式复制空闲时正是依赖这一点周期性唤醒)。
    ///
    /// 分片规则: 包体长度 == `MAX_BODY_LENGTH`(0xFFFFFF) 表示后面还有后续分片，
    /// 需持续拼接直到出现长度 < 0xFFFFFF 的分片。
    pub fn read_packet(&mut self) -> CResult<(Vec<u8>, u8)> {
        loop {
            // 取出一个协议分片并累积(跨多次调用/超时都保留在 self.assembling 中)
            let took_fragment = if let Some((fragment, seq_num)) = self.take_raw_fragment() {
                let fragment_len = fragment.len();
                let assembling = self.assembling.get_or_insert_with(|| AssemblingPacket {
                    payload: Vec::new(),
                    seq_num,
                    last_fragment_len: 0,
                });
                assembling.payload.extend_from_slice(&fragment);
                assembling.seq_num = seq_num;
                assembling.last_fragment_len = fragment_len;
                true
            } else {
                false
            };

            // 末片长度 < MAX_BODY_LENGTH 说明整包已完整，可以交付
            if let Some(assembling) = self.assembling.as_mut() {
                if assembling.last_fragment_len < MAX_BODY_LENGTH {
                    let packet = std::mem::take(&mut assembling.payload);
                    let seq_num = assembling.seq_num;
                    self.assembling = None;
                    return Ok((packet, seq_num));
                }
            }

            // 缓冲区里已无完整分片时才需要继续读 socket；
            // 否则先回到循环顶部消费已缓冲的后续分片(避免无谓阻塞)。
            // 读超时保留已读字节与拼接状态，由调用方决定是否重试。
            if !took_fragment {
                self.read_more()?;
            }
        }
    }

    /// 从缓冲区中取出一个完整的协议分片；不足一个分片时返回 `None`。
    fn take_raw_fragment(&mut self) -> Option<(Vec<u8>, u8)> {
        if self.read_buffer.len() < PACKET_HEADER_SIZE {
            return None;
        }

        let packet_size = match (&self.read_buffer[0..3]).read_u24::<LittleEndian>() {
            Ok(size) => size as usize,
            Err(_) => return None,
        };
        let total_size = PACKET_HEADER_SIZE + packet_size;
        if self.read_buffer.len() < total_size {
            return None;
        }

        let seq_num = self.read_buffer[3];
        let payload = self.read_buffer[PACKET_HEADER_SIZE..total_size].to_vec();
        self.read_buffer.drain(0..total_size);
        Some((payload, seq_num))
    }

    /// 从 socket 读取一批字节追加到缓冲区。
    fn read_more(&mut self) -> CResult<usize> {
        let mut chunk = [0u8; 8 * 1024];
        let size = self.stream.read(&mut chunk)?;
        if size == 0 {
            // 对端关闭连接
            return Err(ReError::IoError(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed by peer",
            )));
        }
        self.read_buffer.extend_from_slice(&chunk[..size]);
        Ok(size)
    }

    /// 写入一个 MySQL 数据包，超过 16MB 时按协议自动分片。
    ///
    /// 分片规则: 载荷长度达到 0xFFFFFF 时分片，且**必须**以一个长度
    /// < 0xFFFFFF 的分片收尾(总长恰为整数倍时补一个 0 长度分片)，
    /// 否则对端会一直等待后续分片。
    pub fn write_packet(&mut self, packet: &[u8], seq_num: u8) -> CResult<()> {
        let mut offset = 0;
        let mut seq = seq_num;

        while packet.len() - offset >= MAX_BODY_LENGTH {
            let end = offset + MAX_BODY_LENGTH;
            self.write_fragment(&packet[offset..end], seq)?;
            offset = end;
            seq = seq.wrapping_add(1);
        }

        // 收尾分片(可能为 0 长度)
        self.write_fragment(&packet[offset..], seq)?;
        Ok(())
    }

    /// 写入单个协议分片
    fn write_fragment(&mut self, fragment: &[u8], seq_num: u8) -> CResult<()> {
        self.stream.write_u24::<LittleEndian>(fragment.len() as u32)?;
        self.stream.write_u8(seq_num)?;
        self.stream.write(fragment)?;
        Ok(())
    }

    /// 升级为 TLS 连接。
    ///
    /// `ssl_opts` 由调用方通过 `ConnectionOptions::effective_ssl_opts()` 解析，
    /// 因此这里不再需要"缺配置就报错"的分支，`IF_AVAILABLE` 才能正常降级。
    pub fn upgrade_to_ssl(self, ssl_opts: &SslOpts, domain: &str) -> CResult<Self> {
        let domain = domain.to_string();

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

        let PacketChannel { stream, read_buffer, assembling } = self;
        match stream {
            ChannelStream::Tcp(tcp_stream) => {
                let secure_stream = match tls_connector.connect(&domain, tcp_stream) {
                    Ok(stream) => stream,
                    Err(err) => {
                        return Err(ReError::ConnectionError(format!(
                            "Can not connect tls. err:{{{err}}}"
                        )))
                    }
                };
                // SSL 升级发生在握手包之后、服务端尚未发送更多数据之时，
                // 缓冲区通常为空；保留其内容以确保不丢字节。
                Ok(Self {
                    stream: ChannelStream::Tls(secure_stream),
                    read_buffer,
                    assembling,
                })
            }
            ChannelStream::Tls(tls_stream) => Ok(Self {
                stream: ChannelStream::Tls(tls_stream),
                read_buffer,
                assembling,
            }),
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

impl fmt::Debug for PacketChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PacketChannel")
            .field("stream", &self.stream)
            .field("buffered_bytes", &self.read_buffer.len())
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    use crate::conn::connection_options::ConnectionOptions;
    use crate::conn::packet_channel::PacketChannel;
    use crate::MAX_BODY_LENGTH;

    /// 读超时发生在"包中途"时，已读字节不能丢失。
    ///
    /// 服务端分两次发送同一个包(中间停顿超过客户端读超时)：
    /// 第一次 `read_packet` 应因超时失败，第二次应能拼出完整且正确的包。
    #[test]
    fn test_read_packet_keep_partial_bytes_on_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            // 包头 4 字节的前 3 字节(包体长度 = 3)
            socket.write_all(&[0x03, 0x00, 0x00]).unwrap();
            socket.flush().unwrap();
            thread::sleep(Duration::from_millis(300));
            // 包头第 4 字节(序号) + 3 字节包体
            socket.write_all(&[0x0a, b'a', b'b', b'c']).unwrap();
            socket.flush().unwrap();
            thread::sleep(Duration::from_millis(200));
        });

        let mut options = ConnectionOptions::default();
        options.hostname = "127.0.0.1".to_string();
        options.port = port;
        options.set_read_timeout(Duration::from_millis(100));

        let mut channel = PacketChannel::new(&options).unwrap();

        // 第一次读取：只有一个半包 -> 读超时
        let first = channel.read_packet();
        let first_err = first.expect_err("first read should time out with a partial packet");
        assert!(first_err.is_timeout(), "first error should be a read timeout: {:?}", first_err);

        // 后续读取：服务端补齐剩余字节前会持续超时；重试直至拼出完整包，
        // 这正是在 forward_events 中的处理方式(超时视为空闲并重试)。
        let mut attempts = 0;
        let (packet, seq_num) = loop {
            match channel.read_packet() {
                Ok(v) => break v,
                Err(e) if e.is_timeout() => {
                    attempts += 1;
                    assert!(attempts < 50, "packet never completed: partial bytes were lost");
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        };

        // 已读的半包头没有丢失，拼出的包完整且正确
        assert_eq!(3, packet.len());
        assert_eq!(b"abc".to_vec(), packet);
        assert_eq!(0x0a, seq_num);

        server.join().unwrap();
    }

    /// >16MB 的包会被拆成多个协议分片，读取时必须重组为一个完整包。
    #[test]
    fn test_read_packet_reassembles_multi_fragment_packet() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        const TAIL: &[u8] = b"HELLO";

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            // 分片 1: 长度恰为 MAX_BODY_LENGTH(0xFFFFFF) => 表示还有后续分片
            socket.write_all(&[0xff, 0xff, 0xff, 0x01]).unwrap();
            let chunk = vec![0x41u8; 64 * 1024];
            let mut sent = 0usize;
            while sent < MAX_BODY_LENGTH {
                let n = (MAX_BODY_LENGTH - sent).min(chunk.len());
                socket.write_all(&chunk[..n]).unwrap();
                sent += n;
            }

            // 分片 2: 长度 5 < 0xFFFFFF => 整包结束(序号递增)
            socket.write_all(&[TAIL.len() as u8, 0x00, 0x00, 0x02]).unwrap();
            socket.write_all(TAIL).unwrap();
            socket.flush().unwrap();
            thread::sleep(Duration::from_millis(200));
        });

        let mut options = ConnectionOptions::default();
        options.hostname = "127.0.0.1".to_string();
        options.port = port;

        let mut channel = PacketChannel::new(&options).unwrap();
        let (packet, seq_num) = channel.read_packet().expect("multi-fragment packet should be reassembled");

        assert_eq!(MAX_BODY_LENGTH + TAIL.len(), packet.len(), "reassembled size mismatch");
        assert_eq!(0x02, seq_num, "seq num should be the last fragment's");
        assert!(
            packet[..MAX_BODY_LENGTH].iter().all(|b| *b == 0x41),
            "first fragment content corrupted"
        );
        assert_eq!(TAIL.to_vec(), packet[MAX_BODY_LENGTH..].to_vec(), "last fragment lost");
        assert!(channel.assembling.is_none(), "assembling state should be cleared");

        server.join().unwrap();
    }

    /// 写入 >16MB 的载荷时必须分片，并以长度 < 0xFFFFFF 的分片收尾。
    #[test]
    fn test_write_packet_splits_large_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut total: Vec<u8> = Vec::new();
            let mut fragments = 0usize;
            let mut last_len = 0usize;

            loop {
                let mut header = [0u8; 4];
                socket.read_exact(&mut header).unwrap();
                let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
                let mut payload = vec![0u8; len];
                socket.read_exact(&mut payload).unwrap();
                total.extend_from_slice(&payload);
                fragments += 1;
                last_len = len;
                if len < MAX_BODY_LENGTH {
                    break;
                }
            }
            (total, fragments, last_len)
        });

        let mut options = ConnectionOptions::default();
        options.hostname = "127.0.0.1".to_string();
        options.port = port;

        let mut channel = PacketChannel::new(&options).unwrap();
        let payload = vec![0x42u8; MAX_BODY_LENGTH + 5];
        channel.write_packet(&payload, 0).unwrap();

        let (total, fragments, last_len) = server.join().unwrap();
        assert_eq!(2, fragments, "payload should be split into 2 fragments");
        assert_eq!(5, last_len, "last fragment must be shorter than MAX_BODY_LENGTH");
        assert_eq!(payload.len(), total.len(), "payload size mismatch after split");
        assert!(total.iter().all(|b| *b == 0x42), "payload content corrupted");
    }
}
