//! A minimal HTTP/1.1 client for one request over an open connection.

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use hyper::header::HOST;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};

/// The largest response body accepted.
const MAX_BODY: usize = 64 * 1024 * 1024;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Sends `request` over `stream`, with `host` as the Host header, and
/// returns the status and body.
pub(crate) async fn send<S>(
    stream: S,
    host: &str,
    mut request: Request<Full<Bytes>>,
) -> Result<(StatusCode, Bytes), BoxError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(connection);
    request.headers_mut().insert(HOST, host.parse()?);
    let response = sender.send_request(request).await?;
    let status = response.status();
    let body = Limited::new(response.into_body(), MAX_BODY)
        .collect()
        .await?
        .to_bytes();
    Ok((status, body))
}

/// Percent-encodes everything but unreserved characters, as a query value.
pub(crate) fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// Sends a GET for `path`.
pub(crate) async fn get<S>(
    stream: S,
    host: &str,
    path: &str,
) -> Result<(StatusCode, Bytes), BoxError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    send(stream, host, Request::get(path).body(Full::default())?).await
}
