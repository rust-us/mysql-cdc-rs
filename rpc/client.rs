use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use memory::Buffer;
use crate::cmd::{Command, identifier_code, REQUEST_MAGIC, RequestType, VERSION};
use crate::cmd::PackageType::REQUEST;
use crate::error::HResult;
use crate::RaftClientConfig;
use crate::session::Session;


type RaftGroupId = u32;

pub struct RaftClient {
    req_id: AtomicI64,
    state: SessionState,
    sessions: HashMap<i32, Session>,
}

struct SessionState {
    client_id: String,
    session_id: i64,
    cluster_id: i64,
}

impl RaftClient {

    pub fn create(config: RaftClientConfig) -> HResult<Self> {

        todo!()
    }

    pub async fn send(cmd: Command) {
        todo!()
    }

    pub async fn send_register(&mut self) -> HResult<()> {
        let mut buf = Buffer::new()?;
        self.init(&mut buf, RequestType::Connect)?;

        Ok(())
    }

    pub async fn send_keep_alive(&mut self) -> HResult<()> {
        let mut buf = Buffer::new()?;
        self.init(&mut buf, RequestType::KeepAlive)?;

        Ok(())
    }

    fn write_common(&mut self, buf: &mut Buffer) -> HResult<()> {
        buf.write_long(REQUEST_MAGIC)?;
        buf.write_int(VERSION)?;
        buf.write_long(self.state.cluster_id)?;
        buf.write_long(self.state.session_id)?;
        Ok(())
    }

    fn init(&mut self, buf: &mut Buffer, req_type: RequestType) -> HResult<()> {
        // request
        buf.write_byte(REQUEST.into())?;
        // req id
        let req_id = self.req_id.fetch_add(1, Ordering::Release);
        buf.write_long(req_id)?;
        // identifier
        let identifier = identifier_code(req_type.into());
        buf.write_byte(identifier)?;
        // type id
        buf.write_int(req_type.into())?;
        Ok(())
    }

    fn write_connect(&mut self, buf: &mut Buffer) -> HResult<()> {
        buf.write_utf8(&self.state.client_id)?;
        Ok(())
    }

}