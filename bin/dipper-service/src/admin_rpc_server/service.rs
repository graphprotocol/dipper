use std::{future::Future, net::SocketAddr};

use dipper_core::state::FromState;
use jsonrpsee::server::{Server, ServerConfig};
use tokio::sync::mpsc;

use super::handlers::{IndexingAgreementsCtx, IndexingRequestsCtx, rpc_handlers};
use crate::{
    registry::{AgreementRegistry, IndexingRequestRegistry, RequestOrderRegistry},
    worker::service::WorkerQueue,
};

/// RPC server configuration.
#[derive(Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
}

/// The RPC server service handle.
///
/// If all handles are dropped, the RPC server will be stopped.
#[derive(Clone)]
pub struct Handle {
    /// A channel to stop the RPC server
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the RPC server
    pub async fn stop(self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Create a new gateway operator API server.
pub fn new<S, R, W>(conf: Config, ctx: S) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: IndexingRequestRegistry
        + RequestOrderRegistry
        + AgreementRegistry
        + Clone
        + Send
        + Sync
        + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
    IndexingRequestsCtx<R, W>: FromState<S>,
    IndexingAgreementsCtx<R>: FromState<S>,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let fut = async move {
        tracing::info!(listen_addr=%conf.listen_addr, "Starting gateway operator API server");

        // Start the RPC server
        let config = ServerConfig::builder()
            .http_only()
            .max_request_body_size(1024 * 1024) // 1 MB
            .set_tcp_no_delay(true)
            .build();

        let server = Server::builder()
            .set_config(config)
            .build(conf.listen_addr)
            .await?;

        let handle = server.start(rpc_handlers(ctx));
        let svc_handle = handle.clone();

        // Wait for either the server to stop, or a stop signal
        tokio::select! {biased;
            _ = svc_handle.stopped() => {}
            _ = rx_stop.recv() => {
                tracing::debug!("Stopping gateway operator API server");

                // Notify the server and wait for it to stop
                if handle.stop().is_ok() {
                    handle.stopped().await;
                }
            }
        }

        Ok(())
    };

    (Handle { tx_stop }, fut)
}
