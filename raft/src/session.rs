use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use memory::Buffer;

use crate::conn::Conn;
use crate::error::HResult;

pub struct Session {
    conn: Conn,
}

impl Session {
    async fn create(addr: String) -> HResult<Self> {
        let tcp = TcpStream::connect(addr).await?;
        Ok(Self {
            conn: Conn::create(tcp)
        })
    }

    async fn send(&mut self, buf: Buffer) -> HResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn test() {
        let handle = tokio::spawn(async {
            println!("hello world");
        });
        let _ = tokio::join!(handle);
    }
}