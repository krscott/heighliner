use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::Router;
use axum::body::Body;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use hyper::body::Frame;
use serde_json::{Value, json};
use tokio::net::{TcpListener, UnixListener};

use super::fake::{Fake, device};
use super::*;

const OWNER: UserId = UserId::new(1);
const OTHER: UserId = UserId::new(2);

/// A status response shaped like tailscaled 1.104's, with fields
/// heighliner ignores.
fn status_json(backend_state: &str) -> Value {
    json!({
        "Version": "1.104.6",
        "BackendState": backend_state,
        "TailscaleIPs": ["100.64.0.1", "fd7a:115c:a1e0::1"],
        "Self": {
            "ID": "nSELF",
            "NodeID": 11,
            "PublicKey": "nodekey:self",
            "HostName": "desktop-os-name",
            "DNSName": "desktop.example.ts.net.",
            "UserID": 1,
            "TailscaleIPs": ["100.64.0.1", "fd7a:115c:a1e0::1"],
            "Online": true,
            "CapMap": {"some-cap": null},
        },
        "Peer": {
            "nodekey:laptop": {
                "ID": "nLAPTOP",
                "HostName": "laptop",
                "DNSName": "laptop.example.ts.net.",
                "UserID": 1,
                "TailscaleIPs": ["100.64.0.2"],
                "Online": false,
            },
            "nodekey:server": {
                "ID": "nSERVER",
                "HostName": "server",
                "DNSName": "server.example.ts.net.",
                "UserID": 99,
                "Tags": ["tag:server"],
                "TailscaleIPs": ["100.64.0.3"],
                "Online": true,
            },
        },
        "User": {"1": {"ID": 1, "LoginName": "someone@example.com"}},
    })
}

fn whois_json() -> Value {
    json!({
        "Node": {
            "ID": 12,
            "StableID": "nLAPTOP",
            "Name": "laptop.example.ts.net.",
            "User": 1,
            "Addresses": ["100.64.0.2/32"],
        },
        "UserProfile": {"ID": 1, "LoginName": "someone@example.com", "DisplayName": "Someone"},
        "CapMap": {},
    })
}

#[test]
fn status_is_read() {
    let status: StatusResponse = serde_json::from_value(status_json("Running")).unwrap();
    let tailnet = status.into_tailnet().unwrap();
    assert_eq!(tailnet.me.id.as_str(), "nSELF");
    assert_eq!(tailnet.me.name, "desktop");
    assert_eq!(tailnet.me.user, OWNER);
    assert_eq!(tailnet.me.ips.len(), 2);
    let owned: Vec<_> = tailnet.owned_peers().map(|d| d.name.as_str()).collect();
    assert_eq!(owned, ["laptop"]);
    assert_eq!(tailnet.peers.len(), 2);
}

#[test]
fn status_needs_a_running_backend() {
    let mut json = status_json("NeedsLogin");
    json["Self"] = Value::Null;
    json["Peer"] = Value::Null;
    let status: StatusResponse = serde_json::from_value(json).unwrap();
    let error = status.into_tailnet().unwrap_err();
    assert!(matches!(&error, Error::NotRunning { state } if state == "NeedsLogin"));
    assert!(error.to_string().contains("tailscale up"), "{error}");
}

#[test]
fn whois_is_read() {
    let whois: WhoIsResponse = serde_json::from_value(whois_json()).unwrap();
    assert_eq!(
        whois.into_identity(),
        Identity {
            node: NodeId::new("nLAPTOP").unwrap(),
            name: "laptop".into(),
            user: OWNER,
        }
    );
}

#[test]
fn query_values_are_percent_encoded() {
    assert_eq!(percent_encode("100.64.0.2:41641"), "100.64.0.2%3A41641");
    assert_eq!(
        percent_encode("[fd7a:115c::1]:80"),
        "%5Bfd7a%3A115c%3A%3A1%5D%3A80"
    );
}

#[test]
fn machine_names() {
    assert_eq!(machine_name("laptop.example.ts.net.", "os-name"), "laptop");
    assert_eq!(machine_name("", "os-name"), "os-name");
}

/// A body sent in chunks, as tailscaled sends status.
struct Chunked(Vec<Bytes>);

impl hyper::body::Body for Chunked {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.0.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(Frame::data(self.0.remove(0)))))
        }
    }
}

/// Serves a fake local API on a unix socket in `dir`.
async fn serve_fake_tailscaled(dir: &Path) -> Client {
    let socket = dir.join("tailscaled.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let check_host = |headers: &HeaderMap| {
        assert_eq!(headers["host"], "local-tailscaled.sock");
    };
    let app = Router::new()
        .route(
            "/localapi/v0/status",
            get(move |headers: HeaderMap| async move {
                check_host(&headers);
                let json = status_json("Running").to_string().into_bytes();
                let (a, b) = json.split_at(json.len() / 2);
                let chunks = vec![Bytes::copy_from_slice(a), Bytes::copy_from_slice(b)];
                Body::new(Chunked(chunks)).into_response()
            }),
        )
        .route(
            "/localapi/v0/whois",
            get(
                move |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| async move {
                    check_host(&headers);
                    match query["addr"].as_str() {
                        "100.64.0.2:41641" => axum::Json(whois_json()).into_response(),
                        "[fd7a:115c:a1e0::9]:1" => {
                            (StatusCode::INTERNAL_SERVER_ERROR, "boom\n").into_response()
                        }
                        _ => (StatusCode::NOT_FOUND, "no match").into_response(),
                    }
                },
            ),
        );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Client::new(socket)
}

#[tokio::test]
async fn client_talks_to_tailscaled() {
    let dir = tempfile::tempdir().unwrap();
    let client = serve_fake_tailscaled(dir.path()).await;

    let tailnet = client.status().await.unwrap();
    assert_eq!(tailnet.me.name, "desktop");

    let laptop = client
        .whois("100.64.0.2:41641".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(laptop.unwrap().name, "laptop");
    assert_eq!(
        client.whois("192.0.2.1:1".parse().unwrap()).await.unwrap(),
        None
    );

    let error = client
        .whois("[fd7a:115c:a1e0::9]:1".parse().unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::Status { status, .. } if *status == StatusCode::INTERNAL_SERVER_ERROR)
    );
    assert!(
        error
            .to_string()
            .ends_with("500 Internal Server Error: boom"),
        "{error}"
    );
}

#[tokio::test]
async fn missing_socket_says_what_to_check() {
    let dir = tempfile::tempdir().unwrap();
    let error = Client::new(dir.path().join("missing.sock"))
        .status()
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Connect { .. }));
    assert!(
        error.to_string().contains("is tailscaled running?"),
        "{error}"
    );
}

#[tokio::test]
async fn discovery_finds_owned_devices_that_listen() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let _ = listener.accept().await;
        }
    });

    let mut fake = Fake::new(device("me", OWNER, "100.64.0.1"));
    // Listens, on its second address.
    let mut listening = device("listening", OWNER, "127.0.0.2");
    listening.ips.push("127.0.0.1".parse().unwrap());
    fake.add_peer(listening);
    // Owned, but nothing listens.
    fake.add_peer(device("idle", OWNER, "127.0.0.3"));
    // Listens, but belongs to someone else.
    fake.add_peer(device("friend", OTHER, "127.0.0.1"));

    let found = discover(&fake, port, Duration::from_secs(1)).await.unwrap();
    let names: Vec<_> = found.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(names, ["listening"]);
}
