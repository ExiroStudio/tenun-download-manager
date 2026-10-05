//! Server probing and remote resource discovery module.

use crate::error::{Result, TenunError};
use crate::platform::sanitize_filename;
use percent_encoding::percent_decode_str;
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;

/// Configuration options for HTTP requests and download behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestOptions {
    /// Custom headers to include in every request.
    pub custom_headers: Vec<(String, String)>,
    /// Custom User-Agent string.
    pub user_agent: Option<String>,
    /// Referer header value.
    pub referer: Option<String>,
    /// Cookie header value.
    pub cookie: Option<String>,
    /// Basic authentication credentials: (username, optional password).
    pub basic_auth: Option<(String, Option<String>)>,
    /// Optional HTTP/HTTPS proxy URL.
    pub proxy: Option<String>,
    /// Timeout duration for requests.
    pub timeout: Option<Duration>,
    /// Number of concurrent connection segments requested (default: 8, max: 32).
    pub connections: Option<usize>,
    /// Minimum size in bytes per segment (default: 1 MiB).
    pub min_segment_size: Option<u64>,
    /// Force HTTP/1.1 connections to prevent multiplexing into a single TCP stream.
    pub http1_only: bool,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            custom_headers: Vec::new(),
            user_agent: None,
            referer: None,
            cookie: None,
            basic_auth: None,
            proxy: None,
            timeout: None,
            connections: Some(crate::segment::DEFAULT_CONNECTIONS),
            min_segment_size: Some(crate::segment::DEFAULT_MIN_SEGMENT_SIZE),
            http1_only: true,
        }
    }
}

/// Metadata discovered about a remote file before downloading.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteInfo {
    /// The initial request URL.
    pub original_url: String,
    /// Final URL after following HTTP redirects.
    pub final_url: String,
    /// Detected and sanitized target filename.
    pub filename: String,
    /// Total file size in bytes, if known.
    pub total_bytes: Option<u64>,
    /// Whether the remote server supports HTTP Range requests.
    pub range_supported: bool,
    /// Entity tag for caching and validation.
    pub etag: Option<String>,
    /// Last modification timestamp string.
    pub last_modified: Option<String>,
    /// MIME Content-Type.
    pub content_type: Option<String>,
}

/// Builds a configured `reqwest::Client` with automatic decompression disabled
/// to ensure consistent byte offsets across range requests.
pub fn build_client(opts: &RequestOptions) -> Result<Client> {
    let mut builder = Client::builder()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .redirect(reqwest::redirect::Policy::limited(10));

    if opts.http1_only {
        builder = builder.http1_only();
    }

    if let Some(ref ua) = opts.user_agent {
        builder = builder.user_agent(ua);
    } else {
        builder = builder.user_agent(concat!("Tenun/", env!("CARGO_PKG_VERSION")));
    }

    if let Some(timeout) = opts.timeout {
        builder = builder.timeout(timeout);
    }

    if let Some(ref proxy_url) = opts.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)
            .map_err(|e| TenunError::Config(format!("Invalid proxy URL: {e}")))?;
        builder = builder.proxy(proxy);
    }

    builder
        .build()
        .map_err(|e| TenunError::Network(e.to_string()))
}

/// Applies configured request options (headers, cookies, auth) to a request builder.
pub fn apply_options(
    mut req: reqwest::RequestBuilder,
    opts: &RequestOptions,
) -> Result<reqwest::RequestBuilder> {
    req = req.header(header::ACCEPT_ENCODING, "identity");

    for (k, v) in &opts.custom_headers {
        let header_name = HeaderName::from_str(k)
            .map_err(|e| TenunError::Config(format!("Invalid header name '{k}': {e}")))?;
        let header_value = HeaderValue::from_str(v)
            .map_err(|_| TenunError::Config(format!("Invalid header value for '{k}'")))?;
        req = req.header(header_name, header_value);
    }

    if let Some(ref referer) = opts.referer {
        req = req.header(header::REFERER, referer);
    }

    if let Some(ref cookie) = opts.cookie {
        req = req.header(header::COOKIE, cookie);
    }

    if let Some((ref user, ref pass)) = opts.basic_auth {
        req = req.basic_auth(user, pass.as_deref());
    }

    Ok(req)
}

/// Probes a remote URL using a `GET` request with `Range: bytes=0-0` to inspect
/// server capabilities, file size, validators, and recommended filename.
pub async fn probe(client: &Client, url: &str, opts: &RequestOptions) -> Result<RemoteInfo> {
    let mut req = client.get(url).header(header::RANGE, "bytes=0-0");
    req = apply_options(req, opts)?;

    let response = req
        .send()
        .await
        .map_err(|e| TenunError::Network(e.to_string()))?;

    process_probe_response(url, response).await
}

async fn process_probe_response(original_url: &str, response: Response) -> Result<RemoteInfo> {
    let status = response.status();
    let final_url = response.url().to_string();
    let headers = response.headers().clone();

    let etag = headers
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let last_modified = headers
        .get(header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let (range_supported, total_bytes) = match status {
        StatusCode::PARTIAL_CONTENT => {
            let total = parse_content_range_total(&headers);
            (true, total)
        }
        StatusCode::OK => {
            let total = headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            (false, total)
        }
        StatusCode::RANGE_NOT_SATISFIABLE => {
            if let Some(cr) = headers
                .get(header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
            {
                if cr.contains("*/0") || cr.ends_with("/0") {
                    (true, Some(0))
                } else {
                    return Err(TenunError::InvalidRange(cr.to_string()));
                }
            } else {
                return Err(TenunError::InvalidRange(
                    "416 Range Not Satisfiable".to_string(),
                ));
            }
        }
        err_status => {
            return Err(TenunError::Network(format!(
                "Probe failed with HTTP status {err_status}"
            )));
        }
    };

    let filename = determine_filename(&headers, &final_url, original_url);

    Ok(RemoteInfo {
        original_url: original_url.to_string(),
        final_url,
        filename,
        total_bytes,
        range_supported,
        etag,
        last_modified,
        content_type,
    })
}

fn parse_content_range_total(headers: &HeaderMap) -> Option<u64> {
    let cr = headers.get(header::CONTENT_RANGE)?.to_str().ok()?;
    let (_, total_str) = cr.split_once('/')?;
    let total_str = total_str.trim();
    if total_str == "*" {
        None
    } else {
        total_str.parse::<u64>().ok()
    }
}

/// Determines the target filename following the precedence:
/// 1. Content-Disposition (`filename*` RFC 5987 / RFC 6266 prioritized over `filename`)
/// 2. Last path segment of final URL (decoded)
/// 3. Fallback: "download"
pub fn determine_filename(headers: &HeaderMap, final_url: &str, original_url: &str) -> String {
    if let Some(cd) = headers
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(name) = parse_content_disposition(cd) {
            let sanitized = sanitize_filename(&name);
            if !sanitized.is_empty() && sanitized != "download" {
                return sanitized;
            }
        }
    }

    if let Some(name) = extract_filename_from_url(final_url) {
        return sanitize_filename(&name);
    }

    if let Some(name) = extract_filename_from_url(original_url) {
        return sanitize_filename(&name);
    }

    "download".to_string()
}

/// Parses Content-Disposition header supporting `filename*` (RFC 5987/6266) and `filename`.
pub fn parse_content_disposition(header_val: &str) -> Option<String> {
    let mut regular_filename = None;
    let mut extended_filename = None;

    for part in header_val.split(';') {
        let trimmed = part.trim();
        if let Some(val) = trimmed.strip_prefix("filename*=") {
            let val = val.trim().trim_matches('"');
            if let Some((_, encoded)) = val.split_once("''") {
                let decoded = percent_decode_str(encoded).decode_utf8_lossy().to_string();
                extended_filename = Some(decoded);
            } else if let Some(idx) = val.find('\'') {
                if let Some(second_idx) = val[idx + 1..].find('\'') {
                    let encoded = &val[idx + 1 + second_idx + 1..];
                    let decoded = percent_decode_str(encoded).decode_utf8_lossy().to_string();
                    extended_filename = Some(decoded);
                }
            }
        } else if let Some(val) = trimmed.strip_prefix("filename=") {
            let val = val.trim();
            let unquoted = if val.starts_with('"') && val.ends_with('"') && val.len() >= 2 {
                &val[1..val.len() - 1]
            } else {
                val
            };
            regular_filename = Some(unquoted.to_string());
        }
    }

    extended_filename.or(regular_filename)
}

/// Extracts the last segment of a URL path, percent-decoded and stripped of query params.
pub fn extract_filename_from_url(url_str: &str) -> Option<String> {
    let clean_url = url_str.split('?').next()?.split('#').next()?;
    let path = clean_url.rsplit_once('/')?.1;
    let decoded = percent_decode_str(path).decode_utf8_lossy().to_string();
    let trimmed = decoded.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_content_disposition_regular() {
        let cd = "attachment; filename=\"ubuntu-24.04-desktop.iso\"";
        assert_eq!(
            parse_content_disposition(cd),
            Some("ubuntu-24.04-desktop.iso".to_string())
        );
    }

    #[test]
    fn test_parse_content_disposition_rfc5987() {
        let cd = "attachment; filename=\"fallback.pdf\"; filename*=UTF-8''%e2%82%ac%20rates.pdf";
        assert_eq!(
            parse_content_disposition(cd),
            Some("€ rates.pdf".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url() {
        assert_eq!(
            extract_filename_from_url(
                "https://example.com/files/archive%20v1.tar.gz?token=abc#frag"
            ),
            Some("archive v1.tar.gz".to_string())
        );
        assert_eq!(extract_filename_from_url("https://example.com/"), None);
    }
}
