use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use futures_util::future::join_all;
use tracing::warn;
use crate::err::DecodeError::ReError;

/// Server have start / shutdown functions
#[async_trait::async_trait]
pub trait Server: Send {

    async fn start(&mut self);

    async fn shutdown(&mut self, graceful: bool) -> Result<(), ReError>;

}

pub struct ShutdownHandle {
    shutdown: Arc<AtomicBool>,
    services: Vec<Box<dyn Server>>,
}

impl ShutdownHandle {

    #[inline]
    pub fn create() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            services: vec![]
        }
    }

    #[inline]
    pub fn add_service(&mut self, server: Box<dyn Server>) {
        self.services.push(server);
    }

    pub async fn shutdown_services(&mut self, graceful: bool) -> Result<(), ReError> {
        let mut futures = vec![];
        for s in &mut self.services {
            futures.push(s.shutdown(graceful));
        }
        // wait all future to complete
        let results = join_all(futures).await;
        // return any error or Ok(())
        results
            .into_iter()
            .find(|r|r.is_err())
            .unwrap_or(Ok(()))

    }

}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        // shutdown server
        if let Ok(false) = self.shutdown.compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire) {
            warn!("server begin to shutdown gracefully");
            let r = futures_executor::block_on(self.shutdown_services(true));
            warn!("server shutdown {:?}", r);
        }
    }
}