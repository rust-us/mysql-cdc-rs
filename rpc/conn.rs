use std::future::Future;
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use byteorder::{BigEndian, ByteOrder};
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use memory::Buffer;

use crate::cmd::{Decoder, Encoder};
use crate::error::Error::{RaftCommandParseFrameErr, RaftCommandParseFrameLenErr};
use crate::error::HResult;

const CONN_REQUEST: i8 = 1;
const CONN_RESPONSE: i8 = 2;

const STATUS_SUCCESS: i8 = 3;
const STATUS_FAILURE: i8 = 4;

/// Response header protocol
/// 4B: Frame length
/// 4B: Header length
///   |-- 4B i32, version
///   |-- 1B i8, code
///   |-- 4B i32, msgType, see MessageType
///   |-- 4B i32, opaque
///   |-- 1B i8, flag
///   |-- 1B bool, remark_nullable. if remark_nullable == false parse Remark
///      |-- Remark, read_utf8, remark
///   |-- 4B i32, seqNo
///   |-- 1B bool, isLast
///   |-- 1B bool, customHeaderIsNull, if customHeaderIsNull == false, parse CustomHeader
///      |-- CustomHeader read_utf8, className
///      |-- CustomHeader 4B i32, custom header len
///      |-- CustomHeader custom header len Byte
/// Body
///   |--
/// 1B: connection type: CONN_REQUEST | CONN_RESPONSE
/// 8B: i64, request id
/// 1B: i8, status: STATUS_SUCCESS | STATUS_FAILURE
pin_project! {
    pub struct Conn {
        #[pin]
        r: OwnedReadHalf,
        #[pin]
        w: OwnedWriteHalf,
        read_state: ReadState,
        read_len: usize,
        frame_len: usize,
        buf: Vec<u8>,
        // #[pin]
        // _pin: PhantomPinned,
    }
}

pin_project! {
    // write remoting command future
    pub struct WriteRemotingCmd<'a> {
        buf: Buffer,
        #[pin]
        w: &'a OwnedWriteHalf,
        seg_idx: usize,
        inner_write: Box<dyn Future<Output = std::io::Result<()>>>,
    }
}

enum ReadState {
    // read 4byte(i32) frame length
    ReadFrameLen,
    // read a full frame packet
    ReadFrame,
}


impl Conn {
    pub fn create(tcp: TcpStream) -> Self {
        let (mut r, mut w) = tcp.into_split();
        let mut buf = Vec::with_capacity(1024);
        unsafe {
            buf.set_len(1024);
        }
        Conn {
            r,
            w,
            buf,
            read_state: ReadState::ReadFrameLen,
            read_len: 0,
            frame_len: 0,
        }
    }
}

impl Future for Conn {
    type Output = HResult<RemotingCommand>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.read_state {
            ReadState::ReadFrameLen => {
                self.poll_read_frame_len(cx)
            }
            ReadState::ReadFrame => {
                self.poll_read_frame(cx)
            }
        }
    }
}

impl Conn {
    pub async fn write(self: &mut Self, cmd: RemotingCommand) -> WriteRemotingCmd {
        let mut buf = Buffer::new()?;
        let _ = cmd.encode(&mut buf)?;
        WriteRemotingCmd::create(buf, &mut self.w)
    }
    fn poll_read_frame_len(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<HResult<RemotingCommand>> {
        let mut me = self.project();
        let read_len = 4 - *me.read_len;
        assert!(read_len > 0, "poll_read_frame_len self.read_len {} is bigger than 4", me.read_len);
        let buf = &mut me.buf[*me.read_len..(*me.read_len + read_len)];
        let mut read_buf = ReadBuf::new(buf);
        match me.r.poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(_)) => {
                return Poll::Ready(Err(RaftCommandParseFrameLenErr));
            }
            Poll::Pending => {
                *me.read_len += read_buf.filled().len();
                return Poll::Pending;
            }
        }
        let frame_len_bytes = &me.buf[0..4];
        *me.frame_len = BigEndian::read_i32(frame_len_bytes) as usize;
        // reset read_len
        *me.read_len = 0;
        assert!(*me.frame_len >= 4, "poll_read_frame_len frame_len {} must ge 4", me.frame_len);
        *me.read_state = ReadState::ReadFrame;
        // try poll, for parse frame body
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn poll_read_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<HResult<RemotingCommand>> {
        let mut me = self.project();
        let read_len = *me.frame_len - 4 - *me.read_len;
        assert!(read_len > 0, "poll_read_frame self.frame_len {}, self.read_len: {}", *me.frame_len, *me.read_len);
        let buf = &mut me.buf[*me.read_len..(*me.read_len + read_len)];
        let mut read_buf = ReadBuf::new(buf);
        match me.r.poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(_)) => {
                return Poll::Ready(Err(RaftCommandParseFrameErr));
            }
            Poll::Pending => {
                *me.read_len += read_buf.filled().len();
                return Poll::Pending;
            }
        }
        *me.read_state = ReadState::ReadFrameLen;
        // try poll, for parse frame length
        cx.waker().wake_by_ref();
        let frame_buf = &mut me.buf[0..*me.frame_len - 4];
        let (cmd, _) = RemotingCommand::decode(frame_buf)?;
        // reset
        *me.frame_len = 0;
        *me.read_len = 0;
        Poll::Ready(Ok(cmd))
    }
}

impl AsyncWrite for Conn {
    #[inline]
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        self.project().w.poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.project().w.poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.project().w.poll_shutdown(cx)
    }
}

impl<'a> WriteRemotingCmd<'a> {
    #[inline]
    fn create(buf: Buffer, w: &'a mut OwnedWriteHalf) -> Self {
        let (idx, offset) = buf.position();
        let seg = buf.segment(0);
        let buffer = if idx == 0 {
            &seg.as_slice()[0..offset]
        } else {
            &seg.as_slice()
        };
        let inner_write = w.write_all(buffer);
        Self {
            buf,
            w,
            seg_idx: 0,
            inner_write: Box::new(inner_write),
        }
    }
}

impl<'a> Future for WriteRemotingCmd<'a> {
    type Output = HResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        loop {
            let _n = ready!(me.inner_write.poll(cx))?;
            let (idx, offset) = me.buf.position();
            // all segment has sent
            if me.seg_idx == idx {
                return Poll::Ready(Ok(()));
            }
            // poll next
            let cur_idx = self.seg_idx + 1;
            let seg = me.buf.segment(cur_idx);
            let buffer = if cur_idx == idx {
                &seg.as_slice()[0..offset]
            } else {
                &seg.as_slice()
            };
            let inner_write = me.w.write_all(buffer);
            *me.inner_write = Box::new(inner_write)
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::net::TcpStream;
    use crate::cmd::RemotingCommand;
    use crate::conn::Conn;
    use crate::error::HResult;

    #[tokio::test]
    async fn test_conn() -> HResult<()> {
        let tcp = TcpStream::connect("127.0.0.1:20006").await?;
        let mut conn = Conn::create(tcp);
        let cmd = RemotingCommand::
        conn.write();
    }

}