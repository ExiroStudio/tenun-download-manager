//! Tests for mock test server functionality.

use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use reqwest::StatusCode;
use std::time::Duration;
use tenun_test_server::{TestServer, TestServerConfig};

#[tokio::test]
async fn test_server_full_download() {
    let test_data = b"Hello, Tenun Download Manager!".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let res = client
        .get(server.download_url())
        .send()
        .await
        .expect("send request");

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.bytes().await.expect("read body");
    assert_eq!(bytes.as_ref(), test_data.as_slice());
}

#[tokio::test]
async fn test_server_range_0_0() {
    let test_data = b"0123456789".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data,
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let res = client
        .get(server.download_url())
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .expect("send request");

    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        res.headers()
            .get(CONTENT_RANGE)
            .expect("content-range header"),
        "bytes 0-0/10"
    );
    assert_eq!(
        res.headers()
            .get(CONTENT_LENGTH)
            .expect("content-length header"),
        "1"
    );

    let bytes = res.bytes().await.expect("read body");
    assert_eq!(bytes.as_ref(), b"0");
}

#[tokio::test]
async fn test_server_range_slice() {
    let test_data = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data,
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let res = client
        .get(server.download_url())
        .header(RANGE, "bytes=3-7")
        .send()
        .await
        .expect("send request");

    assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(res.headers().get(CONTENT_RANGE).unwrap(), "bytes 3-7/26");

    let bytes = res.bytes().await.expect("read body");
    assert_eq!(bytes.as_ref(), b"DEFGH");
}

#[tokio::test]
async fn test_server_range_unsatisfiable() {
    let test_data = b"12345".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data,
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let res = client
        .get(server.download_url())
        .header(RANGE, "bytes=100-200")
        .send()
        .await
        .expect("send request");

    assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(res.headers().get(CONTENT_RANGE).unwrap(), "bytes */5");
}

#[tokio::test]
async fn test_server_disabled_range_fallback_to_200() {
    let test_data = b"Stream content without range support".to_vec();
    let server = TestServer::spawn(TestServerConfig {
        data: test_data.clone(),
        range_enabled: false,
        ..Default::default()
    })
    .await;

    let client = reqwest::Client::new();
    let res = client
        .get(server.download_url())
        .header(RANGE, "bytes=0-4")
        .send()
        .await
        .expect("send request");

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.bytes().await.expect("read body");
    assert_eq!(bytes.as_ref(), test_data.as_slice());
}

#[tokio::test]
async fn test_server_dynamic_reconfiguration() {
    let server = TestServer::spawn(TestServerConfig::default()).await;
    let client = reqwest::Client::new();

    // First request: OK
    let res = client.get(server.download_url()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Modify server config to inject 429 Too Many Requests with Retry-After
    server.modify_config(|cfg| {
        cfg.error_status = Some(StatusCode::TOO_MANY_REQUESTS);
        cfg.retry_after = Some("10".to_string());
    });

    let res = client.get(server.download_url()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        res.headers().get("retry-after").unwrap().to_str().unwrap(),
        "10"
    );

    // Clear error
    server.modify_config(|cfg| {
        cfg.error_status = None;
        cfg.delay = Some(Duration::from_millis(50));
    });

    let res = client.get(server.download_url()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
