//! Configurable HTTP test server for Tenun download manager integration tests.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, Method, StatusCode},
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
    /// Optional RFC 3230 / RFC 5843 Digest header value.
    pub digest: Option<String>,
    /// Optional RFC 1864 Content-MD5 header value.
    pub content_md5: Option<String>,
    /// Artificial delay to introduce before responding.
    pub delay: Option<Duration>,
    /// Artificial error status code to inject (e.g. 500, 429).
    pub error_status: Option<StatusCode>,
    /// Optional Retry-After header value when returning errors.
    pub retry_after: Option<String>,
    /// Bandwidth limit per connection in bytes per second.
    pub speed_limit_bytes_per_sec: Option<u64>,
    /// Automatically return errors for the first N requests before succeeding.
    pub fail_first_n_requests: Option<usize>,
    /// Drop connection abruptly after sending this many bytes.
    pub drop_after_bytes: Option<usize>,
    /// Stall chunk streaming for a duration after sending this many bytes.
    pub stall_after_bytes: Option<(usize, Duration)>,
    /// Only stall on the first data transfer request.
    pub stall_only_first_request: bool,
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
            digest: None,
            content_md5: None,
            delay: None,
            error_status: None,
            retry_after: None,
            speed_limit_bytes_per_sec: None,
            fail_first_n_requests: None,
            drop_after_bytes: None,
            stall_after_bytes: None,
            stall_only_first_request: false,
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
            .route("/file/{id}", get(handle_file_by_id).head(handle_file_by_id))
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

    /// Dynamic file download URL by identifier or filename.
    pub fn file_url(&self, id: &str) -> String {
        format!("http://{}/file/{}", self.addr, id)
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
    let (delay, error_status, retry_after, active_stall) = {
        let mut cfg = config_arc.write().expect("lock poisoned");
        cfg.request_count += 1;
        let count = cfg.request_count;
        let should_fail = if let Some(n) = cfg.fail_first_n_requests {
            count <= n
        } else {
            cfg.error_status.is_some()
        };
        let err = if should_fail {
            cfg.error_status.or(Some(StatusCode::SERVICE_UNAVAILABLE))
        } else {
            None
        };
        let stall = if cfg.stall_only_first_request && count > 2 {
            None
        } else {
            cfg.stall_after_bytes
        };
        (cfg.delay, err, cfg.retry_after.clone(), stall)
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
    if let Some(ref digest) = cfg.digest {
        response_builder = response_builder.header("Digest", digest);
    }
    if let Some(ref md5) = cfg.content_md5 {
        response_builder = response_builder.header("Content-MD5", md5);
    }

    if !cfg.range_enabled || cfg.force_200_for_range {
        response_builder = response_builder.header(header::ACCEPT_RANGES, "none");
        response_builder = response_builder.header(header::CONTENT_LENGTH, total_len.to_string());
        return create_throttled_response(
            response_builder,
            StatusCode::OK,
            cfg.data,
            cfg.speed_limit_bytes_per_sec,
            cfg.drop_after_bytes,
            active_stall,
        );
    }

    response_builder = response_builder.header(header::ACCEPT_RANGES, "bytes");

    // Check If-Range validator if present
    if let Some(if_range) = headers.get(header::IF_RANGE).and_then(|v| v.to_str().ok()) {
        let etag_matches = cfg.etag.as_deref().map(|e| e == if_range).unwrap_or(false);
        let lm_matches = cfg
            .last_modified
            .as_deref()
            .map(|lm| lm == if_range)
            .unwrap_or(false);
        if !etag_matches && !lm_matches {
            // Validator mismatch: ignore Range and reply with 200 OK
            response_builder =
                response_builder.header(header::CONTENT_LENGTH, total_len.to_string());
            return create_throttled_response(
                response_builder,
                StatusCode::OK,
                cfg.data,
                cfg.speed_limit_bytes_per_sec,
                cfg.drop_after_bytes,
                active_stall,
            );
        }
    }

    if let Some(range_header) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(parsed_range) = parse_range_header(range_header, total_len) {
            match parsed_range {
                RangeResult::Satisfiable { start, end } => {
                    let chunk = cfg.data[start..=end].to_vec();
                    let content_range = format!("bytes {}-{}/{}", start, end, total_len);
                    let content_len = (end - start + 1).to_string();

                    response_builder = response_builder
                        .header(header::CONTENT_RANGE, content_range)
                        .header(header::CONTENT_LENGTH, content_len);

                    return create_throttled_response(
                        response_builder,
                        StatusCode::PARTIAL_CONTENT,
                        chunk,
                        cfg.speed_limit_bytes_per_sec,
                        cfg.drop_after_bytes,
                        active_stall,
                    );
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
    response_builder = response_builder.header(header::CONTENT_LENGTH, total_len.to_string());

    create_throttled_response(
        response_builder,
        StatusCode::OK,
        cfg.data,
        cfg.speed_limit_bytes_per_sec,
        cfg.drop_after_bytes,
        active_stall,
    )
}

fn create_throttled_response(
    builder: axum::http::response::Builder,
    status: StatusCode,
    data: Vec<u8>,
    speed_limit: Option<u64>,
    drop_after_bytes: Option<usize>,
    stall_after_bytes: Option<(usize, Duration)>,
) -> Response {
    let chunk_size = 16 * 1024; // 16 KiB chunks
    let chunk_delay = speed_limit
        .filter(|&lim| lim > 0)
        .map(|lim| Duration::from_secs_f64((chunk_size as f64) / (lim as f64)));

    let stream = async_stream::stream! {
        let mut sent = 0usize;
        for chunk in data.chunks(chunk_size) {
            if let Some(drop_limit) = drop_after_bytes {
                if sent >= drop_limit {
                    yield Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "simulated connection reset",
                    ));
                    return;
                }
            }

            if let Some((stall_byte, duration)) = stall_after_bytes {
                if sent >= stall_byte {
                    tokio::time::sleep(duration).await;
                }
            }

            yield Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(chunk));
            sent += chunk.len();

            if let Some(delay) = chunk_delay {
                tokio::time::sleep(delay).await;
            }
        }
    };

    builder
        .status(status)
        .body(Body::from_stream(stream))
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
    let (start_str, end_str) = spec.split_once('-')?;

    if start_str.is_empty() {
        let suffix_len: usize = end_str.parse().ok()?;
        if suffix_len == 0 || total == 0 {
            return Some(RangeResult::Unsatisfiable);
        }
        let start = total.saturating_sub(suffix_len);
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

/// Deterministically generates pseudo-random payload bytes for test files.
pub fn generate_file_content(id: &str, len: usize) -> Vec<u8> {
    let seed = id
        .bytes()
        .fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
    let mut data = Vec::with_capacity(len);
    let mut state = seed.wrapping_add(1);
    for _ in 0..len {
        state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        data.push((state >> 16) as u8);
    }
    data
}

async fn handle_file_by_id(Path(id): Path<String>, method: Method, headers: HeaderMap) -> Response {
    let file_len = 65_536; // 64 KiB
    let data = generate_file_content(&id, file_len);
    let etag = format!("\"etag-{id}\"");

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ETAG, etag)
        .header(header::ACCEPT_RANGES, "bytes");

    if method == Method::HEAD {
        return builder
            .header(header::CONTENT_LENGTH, file_len.to_string())
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    if let Some(range_header) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(parsed_range) = parse_range_header(range_header, file_len) {
            match parsed_range {
                RangeResult::Satisfiable { start, end } => {
                    let chunk = data[start..=end].to_vec();
                    let content_range = format!("bytes {}-{}/{}", start, end, file_len);
                    let content_len = (end - start + 1).to_string();

                    builder = builder
                        .header(header::CONTENT_RANGE, content_range)
                        .header(header::CONTENT_LENGTH, content_len);

                    return create_throttled_response(
                        builder,
                        StatusCode::PARTIAL_CONTENT,
                        chunk,
                        None,
                        None,
                        None,
                    );
                }
                RangeResult::Unsatisfiable => {
                    let content_range = format!("bytes */{}", file_len);
                    return Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .header(header::CONTENT_RANGE, content_range)
                        .body(Body::empty())
                        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
                }
            }
        }
    }

    builder = builder.header(header::CONTENT_LENGTH, file_len.to_string());
    create_throttled_response(builder, StatusCode::OK, data, None, None, None)
}
