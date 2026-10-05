//! Configurable HTTP test server for Tenun download manager integration tests.

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Configuration options for the mock test server.
#[derive(Debug, Clone)]
pub struct TestServerConfig {
    /// File data to be served.
    pub data: Vec<u8>,
    /// Whether HTTP Range requests are accepted.
    pub range_enabled: bool,
    /// If true, returns 200 OK with full content even when Range is requested.
    pub force_200_for_range: bool,
    /// Optional ETag value (e.g. `"test-etag-v1"`).
    pub etag: Option<String>,
    /// Optional Last-Modified header value.
    pub last_modified: Option<String>,
    /// Content-Type header.
    pub content_type: String,
    /// Content-Disposition header.
    pub content_disposition: Option<String>,
    /// Artificial delay to introduce before responding.
    pub delay: Option<Duration>,
    /// Artificial error status code to inject (e.g. 500, 429).
    pub error_status: Option<StatusCode>,
    /// Optional Retry-After header value when returning errors.
    pub retry_after: Option<String>,
    /// Number of requests received so far.
    pub request_count: usize,
}

impl Default for TestServerConfig {
    fn default() -> Self {
        Self {
            data: b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec(),
            range_enabled: true,
            force_200_for_range: false,
            etag: Some("\"initial-etag\"".to_string()),
            last_modified: Some("Mon, 05 Oct 2026 12:00:00 GMT".to_string()),
            content_type: "application/octet-stream".to_string(),
            content_disposition: None,
            delay: None,
            error_status: None,
            retry_after: None,
            request_count: 0,
        }
    }
}

/// A running instance of the test server.
pub struct TestServer {
    addr: SocketAddr,
    config: Arc<RwLock<TestServerConfig>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TestServer {
    /// Spawns a new test server on an ephemeral port.
    pub async fn spawn(config: TestServerConfig) -> Self {
        let config_arc = Arc::new(RwLock::new(config));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind ephemeral test server port");
        let addr = listener.local_addr().expect("Failed to get local address");

        let app_state = Arc::clone(&config_arc);
        let app = Router::new()
            .route("/download", get(handle_download).head(handle_download))
            .with_state(app_state);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        Self {
            addr,
            config: config_arc,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Base download URL.
    pub fn download_url(&self) -> String {
        format!("http://{}/download", self.addr)
    }

    /// Server network address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Update the server configuration during test execution.
    pub fn modify_config<F>(&self, modify: F)
    where
        F: FnOnce(&mut TestServerConfig),
    {
        let mut cfg = self.config.write().expect("lock poisoned");
        modify(&mut cfg);
    }

    /// Total number of requests received by the server.
    pub fn request_count(&self) -> usize {
        self.config.read().expect("lock poisoned").request_count
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn handle_download(
    State(config_arc): State<Arc<RwLock<TestServerConfig>>>,
    headers: HeaderMap,
) -> Response {
    let (delay, error_status, retry_after) = {
        let mut cfg = config_arc.write().expect("lock poisoned");
        cfg.request_count += 1;
        (cfg.delay, cfg.error_status, cfg.retry_after.clone())
    };

    if let Some(d) = delay {
        tokio::time::sleep(d).await;
    }

    if let Some(status) = error_status {
        let mut res = Response::builder().status(status);
        if let Some(ra) = retry_after {
            res = res.header(header::RETRY_AFTER, ra);
        }
        return res
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    let cfg = config_arc.read().expect("lock poisoned").clone();
    let total_len = cfg.data.len();

    let mut response_builder = Response::builder();
    response_builder = response_builder.header(header::CONTENT_TYPE, &cfg.content_type);

    if let Some(ref etag) = cfg.etag {
        response_builder = response_builder.header(header::ETAG, etag);
    }
    if let Some(ref lm) = cfg.last_modified {
        response_builder = response_builder.header(header::LAST_MODIFIED, lm);
    }
    if let Some(ref cd) = cfg.content_disposition {
        response_builder = response_builder.header(header::CONTENT_DISPOSITION, cd);
    }

    if !cfg.range_enabled || cfg.force_200_for_range {
        response_builder = response_builder.header(header::ACCEPT_RANGES, "none");
        response_builder = response_builder.header(header::CONTENT_LENGTH, total_len.to_string());
        return response_builder
            .status(StatusCode::OK)
            .body(Body::from(cfg.data))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    response_builder = response_builder.header(header::ACCEPT_RANGES, "bytes");

    if let Some(range_header) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(parsed_range) = parse_range_header(range_header, total_len) {
            match parsed_range {
                RangeResult::Satisfiable { start, end } => {
                    let chunk = &cfg.data[start..=end];
                    let content_range = format!("bytes {}-{}/{}", start, end, total_len);
                    let content_len = (end - start + 1).to_string();

                    response_builder = response_builder
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, content_range)
                        .header(header::CONTENT_LENGTH, content_len);

                    return response_builder
                        .body(Body::from(Bytes::copy_from_slice(chunk)))
                        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
                }
                RangeResult::Unsatisfiable => {
                    let content_range = format!("bytes */{}", total_len);
                    return Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .header(header::CONTENT_RANGE, content_range)
                        .body(Body::empty())
                        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
                }
            }
        }
    }

    // Default full content response
    response_builder = response_builder
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, total_len.to_string());

    response_builder
        .body(Body::from(cfg.data))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

enum RangeResult {
    Satisfiable { start: usize, end: usize },
    Unsatisfiable,
}

fn parse_range_header(range_str: &str, total: usize) -> Option<RangeResult> {
    let range_str = range_str.trim();
    if !range_str.starts_with("bytes=") {
        return None;
    }
    let spec = &range_str["bytes=".len()..];
    // Support single range "start-end"
    let (start_str, end_str) = spec.split_once('-')?;

    if start_str.is_empty() {
        // Suffix range: -N means last N bytes
        let suffix_len: usize = end_str.parse().ok()?;
        if suffix_len == 0 {
            return Some(RangeResult::Unsatisfiable);
        }
        let start = total.saturating_sub(suffix_len);
        if total == 0 {
            return Some(RangeResult::Unsatisfiable);
        }
        Some(RangeResult::Satisfiable {
            start,
            end: total.saturating_sub(1),
        })
    } else {
        let start: usize = start_str.parse().ok()?;
        if start >= total {
            return Some(RangeResult::Unsatisfiable);
        }
        let end = if end_str.is_empty() {
            total.saturating_sub(1)
        } else {
            let parsed_end: usize = end_str.parse().ok()?;
            parsed_end.min(total.saturating_sub(1))
        };

        if start > end {
            return Some(RangeResult::Unsatisfiable);
        }

        Some(RangeResult::Satisfiable { start, end })
    }
}
