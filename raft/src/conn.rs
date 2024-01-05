use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll, ready};

use byteorder::{BE, ByteOrder};
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWriteExt, BufReader, BufWriter, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use memory::Buffer;

use crate::cmd::{Command, Decoder, Encoder};
use crate::cmd::PackageType::REQUEST;
use crate::error::HResult;

type CReader = BufReader<OwnedReadHalf>;
type CWriter = BufWriter<OwnedWriteHalf>;

pin_project! {
    pub struct Conn {
        #[pin]
        r: CReader,
        #[pin]
        w: CWriter,
        read_buf: Vec<u8>,
        read_state: ReadState,
        read_len: usize,
        body_len: usize,
        req_state: ReqState,
    }
}

struct ReqState {
    req_id: AtomicI64,
}

enum ReadState {
    ReadHead,
    ReadBody,
}

const INIT_LENGTH: usize = 256;

impl Conn {
    pub fn create(tcp: TcpStream) -> Self {
        let (r, w) = tcp.into_split();
        let mut read_buf = Vec::with_capacity(INIT_LENGTH);
        unsafe {
            read_buf.set_len(INIT_LENGTH);
        }
        Self {
            r: BufReader::with_capacity(1024, r),
            w: BufWriter::with_capacity(4096, w),
            read_buf,
            read_state: ReadState::ReadHead,
            read_len: 0,
            body_len: 0,
            req_state: ReqState::default()
        }
    }

    pub async fn write(self: &mut Self, cmd: Command) -> HResult<()> {
        let mut buf = Buffer::new()?;
        // write command header
        buf.write_byte(REQUEST.into())?;
        let req_id = self.req_state.req_id.fetch_add(1, Ordering::Acquire);
        buf.write_long(req_id)?;
        // identifier

        // write command body
        let _ = cmd.encode(&mut buf)?;

        // step1, write package head len, i32
        self.w.write_i32(buf.length() as i32).await?;
        if cfg!(debug_assertions) {
            println!("send 4 bytes: \t: {:?}", (buf.length() as i32).to_be_bytes());
        }
        let (idx, offset) = buf.position();
        for i in 0..=idx {
            let seg = buf.segment(i);
            if i < idx {
                if cfg!(debug_assertions) {
                    let s = seg.as_slice();
                    println!("send {} bytes: {:?}\t", s.len(), s);
                }
                self.w.write_all(seg.as_slice()).await?;
            } else {
                if cfg!(debug_assertions) {
                    let s = &seg.as_slice()[0..offset];
                    println!("send {} bytes: {:?}\t", s.len(), s);
                }
                self.w.write_all(&seg.as_slice()[0..offset]).await?;
            }
        }
        // flush bytes to network io
        self.w.flush().await?;
        Ok(())
    }
}

impl Future for Conn {
    type Output = HResult<Command>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        loop {
            match me.read_state {
                ReadState::ReadHead => {
                    let body_len = ready!(poll_head(Pin::new(&mut *me.r), cx, me.read_len, me.read_buf));
                    *me.body_len = body_len;
                    *me.read_state = ReadState::ReadBody;
                    *me.read_len = 0;
                    ensure_cap(me.read_buf, body_len);
                }
                ReadState::ReadBody => {
                    let result = ready!(
                        poll_body(Pin::new(&mut *me.r), cx, me.read_len, *me.body_len, me.read_buf)
                    );
                    *me.body_len = 0;
                    *me.read_state = ReadState::ReadHead;
                    *me.read_len = 0;
                    ensure_cap(me.read_buf, 2);
                    return Poll::Ready(result);
                }
            }
        }
    }
}

fn poll_head(mut r: Pin<&mut CReader>, cx: &mut Context<'_>, offset: &mut usize, buf: &mut Vec<u8>) -> Poll<usize> {
    let need_read_len = 4 - *offset;
    assert!(need_read_len > 0, "conn poll_head need_read_len {} must be positive", need_read_len);
    let mut read_buf = ReadBuf::new(&mut buf[*offset..need_read_len]);
    let read_result = r.poll_read(cx, &mut read_buf);
    match read_result {
        Poll::Pending => {
            let fill_len = read_buf.filled().len();
            *offset += fill_len;
            return Poll::Pending;
        }
        Poll::Ready(_) => {
            let body_len = BE::read_i32(buf.as_slice()) as usize;
            // trigger next poll
            cx.waker().wake_by_ref();
            Poll::Ready(body_len)
        }
    }
}

fn poll_body(mut r: Pin<&mut CReader>, cx: &mut Context<'_>, offset: &mut usize, body_len: usize, buf: &mut Vec<u8>) -> Poll<HResult<Command>> {
    let need_read_len = body_len - *offset;
    assert!(need_read_len > 0, "conn poll_body need_read_len {} must be positive", need_read_len);
    let mut read_buf = ReadBuf::new(&mut buf[*offset..need_read_len]);
    let read_result = r.poll_read(cx, &mut read_buf);
    match read_result {
        Poll::Pending => {
            let fill_len = read_buf.filled().len();
            *offset += fill_len;
            return Poll::Pending;
        }
        Poll::Ready(_) => {
            let bin = &buf[0..body_len];
            Poll::Ready(Command::decode(bin).map(|(c, _)| c))
        }
    }
}

#[inline]
fn ensure_cap(buf: &mut Vec<u8>, cap: usize) {
    if buf.capacity() < cap {
        buf.reserve(cap);
    }
    unsafe {
        buf.set_len(cap);
    }
}

impl Default for ReqState {
    #[inline]
    fn default() -> Self {
        Self {
            req_id: AtomicI64::new(0),
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::time::sleep;
    use crate::cmd::Command;
    use crate::conn::Conn;
    use crate::error::HResult;
    use crate::serde::connect::ConnectRequest;

    #[tokio::test]
    async fn test_conn() -> HResult<()> {
        let tcp = TcpStream::connect("127.0.1.1:20006").await?;
        let mut conn = Conn::create(tcp);
        let cmd = Command::ConnectRequest(ConnectRequest {
            client: String::from("123")
        });
        conn.write(cmd).await?;
        sleep(Duration::from_secs(5)).await;
        Ok(())
    }

}

