//! Cryptographic checksum, streaming hashing, and HTTP header verification.

use crate::error::{Result, TenunError};
use base64::prelude::*;
use md5::Md5;
use reqwest::header::{self, HeaderMap};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Supported hashing algorithms for checksum verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    /// SHA-256 (256-bit, 64 hex characters).
    Sha256,
    /// SHA-1 (160-bit, 40 hex characters).
    Sha1,
    /// MD5 (128-bit, 32 hex characters).
    Md5,
}

impl std::fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sha256 => write!(f, "SHA-256"),
            Self::Sha1 => write!(f, "SHA-1"),
            Self::Md5 => write!(f, "MD5"),
        }
    }
}

/// Strategy for resolving file name collisions when destination file already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ClashResolution {
    /// Appends incrementing number: `file (1).ext`, `file (2).ext`.
    #[default]
    Rename,
    /// Overwrites existing file at target location.
    Overwrite,
    /// Skips downloading if existing file has identical checksum (deduplication).
    Deduplicate,
}

/// Expected checksum specification for integrity verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedChecksum {
    /// Hashing algorithm used.
    pub algorithm: ChecksumAlgorithm,
    /// Hexadecimal digest string (case-insensitive).
    pub hex_digest: String,
}

impl ExpectedChecksum {
    /// Creates a new `ExpectedChecksum`, normalizing the hex digest to lowercase.
    pub fn new(algorithm: ChecksumAlgorithm, hex_digest: &str) -> Self {
        Self {
            algorithm,
            hex_digest: hex_digest.trim().to_ascii_lowercase(),
        }
    }

    /// Verifies if the computed actual digest matches this expected checksum.
    pub fn matches(&self, actual_hex: &str) -> bool {
        self.hex_digest.eq_ignore_ascii_case(actual_hex.trim())
    }
}

/// Computes the cryptographic checksum of a file on disk using streaming reading
/// with a 64 KiB buffer to maintain constant $O(1)$ memory usage.
pub fn compute_file_checksum(path: &Path, algorithm: ChecksumAlgorithm) -> Result<String> {
    let mut file = File::open(path).map_err(TenunError::Io)?;
    let mut buffer = [0u8; 64 * 1024]; // 64 KiB buffer

    match algorithm {
        ChecksumAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            loop {
                let n = file.read(&mut buffer).map_err(TenunError::Io)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
        ChecksumAlgorithm::Sha1 => {
            let mut hasher = Sha1::new();
            loop {
                let n = file.read(&mut buffer).map_err(TenunError::Io)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
        ChecksumAlgorithm::Md5 => {
            let mut hasher = Md5::new();
            loop {
                let n = file.read(&mut buffer).map_err(TenunError::Io)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            Ok(hex::encode(hasher.finalize()))
        }
    }
}

/// Verifies whether the file at `path` matches the `expected` checksum.
pub fn verify_file_checksum(path: &Path, expected: &ExpectedChecksum) -> Result<bool> {
    let actual_hex = compute_file_checksum(path, expected.algorithm)?;
    Ok(expected.matches(&actual_hex))
}

/// Extracts an expected checksum from standard HTTP response headers (`Digest`, `Content-MD5`, or `ETag`).
pub fn extract_checksum_from_headers(headers: &HeaderMap) -> Option<ExpectedChecksum> {
    // 1. Try RFC 3230 / RFC 5843 / RFC 9530 Digest header
    if let Some(digest_val) = headers.get("Digest").and_then(|v| v.to_str().ok()) {
        if let Some(chk) = parse_digest_header(digest_val) {
            return Some(chk);
        }
    }

    // 2. Try RFC 1864 Content-MD5 header
    if let Some(md5_val) = headers.get("Content-MD5").and_then(|v| v.to_str().ok()) {
        if let Some(chk) = parse_content_md5_header(md5_val) {
            return Some(chk);
        }
    }

    // 3. Try standard Amazon S3 / Apache 32-character hex ETag
    if let Some(etag_val) = headers.get(header::ETAG).and_then(|v| v.to_str().ok()) {
        if let Some(chk) = parse_etag_checksum(etag_val) {
            return Some(chk);
        }
    }

    None
}

/// Parses an RFC 3230 `Digest` header value (e.g. `SHA-256=X48E9qOokqqrvdts8nOJRJN3OWDUoyWxBf7kbu9DBPE=`).
pub fn parse_digest_header(digest: &str) -> Option<ExpectedChecksum> {
    for part in digest.split(',') {
        let part = part.trim();
        if let Some((algo_str, hash_str)) = part.split_once('=') {
            let algo_str = algo_str.trim().to_ascii_lowercase();
            let hash_str = hash_str.trim();

            let algorithm = match algo_str.as_str() {
                "sha-256" | "sha256" => ChecksumAlgorithm::Sha256,
                "sha-1" | "sha1" | "sha" => ChecksumAlgorithm::Sha1,
                "md5" => ChecksumAlgorithm::Md5,
                _ => continue,
            };

            // Can be Base64-encoded or already hex-encoded
            let hex_digest = if let Ok(bytes) = BASE64_STANDARD.decode(hash_str) {
                hex::encode(bytes)
            } else if hash_str.chars().all(|c| c.is_ascii_hexdigit()) {
                hash_str.to_ascii_lowercase()
            } else {
                continue;
            };

            return Some(ExpectedChecksum::new(algorithm, &hex_digest));
        }
    }
    None
}

/// Parses an RFC 1864 `Content-MD5` header value (base64-encoded 16-byte MD5 digest).
pub fn parse_content_md5_header(content_md5: &str) -> Option<ExpectedChecksum> {
    let bytes = BASE64_STANDARD.decode(content_md5.trim()).ok()?;
    if bytes.len() == 16 {
        Some(ExpectedChecksum::new(
            ChecksumAlgorithm::Md5,
            &hex::encode(bytes),
        ))
    } else {
        None
    }
}

/// Parses standard 32-hex (MD5) or 64-hex (SHA-256) ETags.
pub fn parse_etag_checksum(etag: &str) -> Option<ExpectedChecksum> {
    let clean = etag
        .trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .trim();

    // S3 multipart ETags have dashes like "c4ca4238a0b923820dcc509a6f75849b-2", which aren't raw hashes
    if clean.contains('-') {
        return None;
    }

    if clean.len() == 32 && clean.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(ExpectedChecksum::new(ChecksumAlgorithm::Md5, clean))
    } else if clean.len() == 64 && clean.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(ExpectedChecksum::new(ChecksumAlgorithm::Sha256, clean))
    } else {
        None
    }
}

/// Parses a GNU coreutils format checksum file (such as `SHA256SUMS` or `MD5SUMS`)
/// looking for the entry corresponding to `target_filename`.
pub fn parse_checksum_file(content: &str, target_filename: &str) -> Option<ExpectedChecksum> {
    let target_basename = Path::new(target_filename)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(target_filename);

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            let hash = parts[0];
            let raw_path = parts[1].trim_start_matches('*');
            let filename = Path::new(raw_path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(raw_path);

            if filename.eq_ignore_ascii_case(target_basename) {
                let algorithm = match hash.len() {
                    64 => ChecksumAlgorithm::Sha256,
                    40 => ChecksumAlgorithm::Sha1,
                    32 => ChecksumAlgorithm::Md5,
                    _ => continue,
                };

                if hash.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(ExpectedChecksum::new(algorithm, hash));
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_streaming_hashing_correctness() {
        let mut temp = NamedTempFile::new().unwrap();
        use std::io::Write;
        temp.write_all(b"Hello Tenun Download Manager!").unwrap();

        let sha256 = compute_file_checksum(temp.path(), ChecksumAlgorithm::Sha256).unwrap();
        let expected_sha256 = "feffa62d6206da926c500bcf65f1dac638769448c7a654b72fabdda7a3a02b25";
        assert_eq!(sha256, expected_sha256);

        let sha1 = compute_file_checksum(temp.path(), ChecksumAlgorithm::Sha1).unwrap();
        assert_eq!(sha1.len(), 40);

        let md5 = compute_file_checksum(temp.path(), ChecksumAlgorithm::Md5).unwrap();
        assert_eq!(md5.len(), 32);

        let expected = ExpectedChecksum::new(ChecksumAlgorithm::Sha256, expected_sha256);
        assert!(verify_file_checksum(temp.path(), &expected).unwrap());

        let wrong = ExpectedChecksum::new(ChecksumAlgorithm::Sha256, &"0".repeat(64));
        assert!(!verify_file_checksum(temp.path(), &wrong).unwrap());
    }

    #[test]
    fn test_parse_digest_header() {
        let raw_hex = "feffa62d6206da926c500bcf65f1dac638769448c7a654b72fabdda7a3a02b25";
        let raw_bytes = hex::decode(raw_hex).unwrap();
        let b64 = BASE64_STANDARD.encode(&raw_bytes);
        let digest_header = format!("SHA-256={b64}");

        let checksum = parse_digest_header(&digest_header).expect("valid digest header");
        assert_eq!(checksum.algorithm, ChecksumAlgorithm::Sha256);
        assert_eq!(checksum.hex_digest, raw_hex);
    }

    #[test]
    fn test_parse_content_md5_header() {
        // "Q2hlY2sgSW50ZWdyaXR5IQ==" -> 16 bytes MD5
        let raw = b"1234567890123456";
        let b64 = BASE64_STANDARD.encode(raw);
        let checksum = parse_content_md5_header(&b64).expect("valid content-md5");
        assert_eq!(checksum.algorithm, ChecksumAlgorithm::Md5);
        assert_eq!(checksum.hex_digest, hex::encode(raw));
    }

    #[test]
    fn test_parse_etag_checksum() {
        // Standard MD5 ETag
        let etag = "\"c4ca4238a0b923820dcc509a6f75849b\"";
        let chk = parse_etag_checksum(etag).expect("valid etag md5");
        assert_eq!(chk.algorithm, ChecksumAlgorithm::Md5);
        assert_eq!(chk.hex_digest, "c4ca4238a0b923820dcc509a6f75849b");

        // S3 multipart ETag should be ignored
        let s3_multipart = "\"c4ca4238a0b923820dcc509a6f75849b-3\"";
        assert!(parse_etag_checksum(s3_multipart).is_none());
    }

    #[test]
    fn test_parse_checksum_file() {
        let content = r#"
# Checksum file test
66f44d8b9487c699faaa4687d60f5e7146522cbbcf074e6a88b502cf3c95963b  archive.tar.gz
c4ca4238a0b923820dcc509a6f75849b *image.iso
"#;

        let chk1 = parse_checksum_file(content, "archive.tar.gz").expect("found archive");
        assert_eq!(chk1.algorithm, ChecksumAlgorithm::Sha256);
        assert_eq!(
            chk1.hex_digest,
            "66f44d8b9487c699faaa4687d60f5e7146522cbbcf074e6a88b502cf3c95963b"
        );

        let chk2 = parse_checksum_file(content, "image.iso").expect("found image");
        assert_eq!(chk2.algorithm, ChecksumAlgorithm::Md5);
        assert_eq!(chk2.hex_digest, "c4ca4238a0b923820dcc509a6f75849b");

        assert!(parse_checksum_file(content, "nonexistent.zip").is_none());
    }
}
