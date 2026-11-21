//! Authorization: every request, from another Node, the CLI or the web UI,
//! must come from a device owned by the user who owns this Node.
//!
//! Tailscale's WhoIs identifies the device behind a connection, so this
//! needs no keys of its own. It relies on the caller's address being its
//! real tailnet address, so servers listen only on this Node's Tailscale
//! addresses. Anything that sends from an owned device's tailnet address
//! passes, including other local users and traffic the device forwards;
//! Nodes are single-user devices.

use std::convert::Infallible;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Extension, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::model::NodeId;
use crate::tailscale::{self, Identity, LocalApi, Owner, Tailnet};

/// Checks callers against the user who owns this Node.
///
/// The owner is read when the Authorizer is created. Each request also
/// checks that this device still has that owner, so while its login or
/// tags differ, every request is refused.
pub struct Authorizer<T> {
    api: T,
    me: NodeId,
    owner: Owner,
}

/// Why a caller was refused.
#[derive(Debug)]
pub enum Denied {
    /// The address isn't a tailnet device.
    NotOnTailnet(SocketAddr),
    /// The device belongs to another user, or is tagged.
    NotOwned(Identity),
    /// tailscaled couldn't be asked; requests are refused until it can.
    Lookup(tailscale::Error),
    /// This device's Tailscale login or tags changed since heighliner
    /// started.
    OwnerChanged,
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotOnTailnet(addr) => write!(f, "{addr} is not on the tailnet"),
            Self::NotOwned(identity) => write!(
                f,
                "{} ({}) is not owned by this Node's user",
                identity.name, identity.node
            ),
            Self::Lookup(error) => write!(f, "can't identify the caller: {error}"),
            Self::OwnerChanged => f.write_str(
                "this device's Tailscale login or tags changed since heighliner started; restart heighliner",
            ),
        }
    }
}

impl Denied {
    /// The status and body to answer with. Lookup details stay in the log,
    /// away from unidentified callers.
    fn answer(&self) -> (StatusCode, String) {
        match self {
            Self::NotOnTailnet(_) | Self::NotOwned(_) => (StatusCode::FORBIDDEN, self.to_string()),
            Self::Lookup(_) => {
                tracing::warn!("{self}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "can't identify the caller right now; see heighliner's log".to_owned(),
                )
            }
            Self::OwnerChanged => {
                tracing::error!("{self}");
                (StatusCode::SERVICE_UNAVAILABLE, self.to_string())
            }
        }
    }
}

impl IntoResponse for Denied {
    fn into_response(self) -> Response {
        self.answer().into_response()
    }
}

impl<T: LocalApi> Authorizer<T> {
    /// Reads this Node's owner from `api`. Fails if this device is tagged.
    pub async fn new(api: T) -> Result<Self, tailscale::Error> {
        let tailnet = api.status().await?;
        Ok(Self::for_tailnet(api, &tailnet))
    }

    /// Uses the owner in a status `api` just gave.
    pub fn for_tailnet(api: T, tailnet: &Tailnet) -> Self {
        Self {
            owner: tailnet.owner(),
            me: tailnet.me().id.clone(),
            api,
        }
    }

    /// Identifies the caller at `addr`, if it may make requests.
    pub async fn authorize(&self, addr: SocketAddr) -> Result<Identity, Denied> {
        let identity = match self.api.whois(addr).await {
            Ok(Some(identity)) if self.owner.owns(identity.user, identity.tagged) => identity,
            Ok(Some(identity)) => return Err(Denied::NotOwned(identity)),
            Ok(None) => return Err(Denied::NotOnTailnet(addr)),
            Err(error) => return Err(Denied::Lookup(error)),
        };
        self.check_owner().await?;
        Ok(identity)
    }

    /// Checks that this device is still the one, with the same owner, that
    /// the Authorizer was created for.
    async fn check_owner(&self) -> Result<(), Denied> {
        let me = self.api.this_device().await.map_err(Denied::Lookup)?;
        if me.id != self.me || !self.owner.owns(me.user, me.tagged) {
            return Err(Denied::OwnerChanged);
        }
        Ok(())
    }
}

/// Serves `router` on `listener` to the callers `authorizer` accepts.
/// Handlers get the caller's [`Identity`] as a request extension.
///
/// Each connection is authorized as it is accepted, before anything is
/// read from it; a refused caller gets a single error response and the
/// connection is closed. Each request on an accepted connection checks
/// again that this device still has its owner. A caller that doesn't send
/// a request's headers within [`HEADER_TIMEOUT`] is disconnected.
pub async fn serve<T: LocalApi>(
    listener: TcpListener,
    router: Router,
    authorizer: Arc<Authorizer<T>>,
) -> Infallible {
    serve_with(listener, router, authorizer, HEADER_TIMEOUT).await
}

/// How long a caller has to send a request's headers.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// How many connections may be waiting to be authorized or being refused
/// at once. Beyond that, new connections are closed at once, so callers
/// that aren't the owner's can't use up file descriptors.
const MAX_UNAUTHORIZED: usize = 256;

/// How long a refused connection is kept open for the caller to read the
/// answer.
const REFUSE_LINGER: Duration = Duration::from_secs(1);

async fn serve_with<T: LocalApi>(
    listener: TcpListener,
    router: Router,
    authorizer: Arc<Authorizer<T>>,
    header_timeout: Duration,
) -> Infallible {
    let router = router.layer(middleware::from_fn_with_state(
        authorizer.clone(),
        still_owned,
    ));
    let unauthorized = Arc::new(Semaphore::new(MAX_UNAUTHORIZED));
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                // For example, out of file descriptors; try again shortly.
                tracing::warn!(%error, "can't accept a connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Ok(permit) = unauthorized.clone().try_acquire_owned() else {
            tracing::debug!(%addr, "too many unauthorized connections; closing");
            continue;
        };
        let authorizer = authorizer.clone();
        let router = router.clone();
        tokio::spawn(async move {
            match authorizer.authorize(addr).await {
                Ok(identity) => {
                    drop(permit);
                    let service = TowerToHyperService::new(router.layer(Extension(identity)));
                    let connection = http1::Builder::new()
                        .timer(TokioTimer::new())
                        .header_read_timeout(header_timeout)
                        .serve_connection(TokioIo::new(stream), service);
                    if let Err(error) = connection.await {
                        tracing::debug!(%addr, %error, "connection ended");
                    }
                }
                Err(denied) => refuse(stream, &denied).await,
            }
        });
    }
}

/// Sends `denied`'s answer without reading the request, then closes.
async fn refuse(mut stream: TcpStream, denied: &Denied) {
    let (status, body) = denied.answer();
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let send = async {
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await?;
        // Read what the caller sent until it closes, so that closing doesn't
        // reset the connection before the caller reads the answer.
        let mut discard = [0; 4096];
        while stream.read(&mut discard).await? > 0 {}
        io::Result::Ok(())
    };
    let _ = tokio::time::timeout(REFUSE_LINGER, send).await;
}

async fn still_owned<T: LocalApi>(
    State(authorizer): State<Arc<Authorizer<T>>>,
    request: Request,
    next: Next,
) -> Response {
    match authorizer.check_owner().await {
        Ok(()) => next.run(request).await,
        Err(denied) => denied.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::Extension;
    use axum::routing::get;
    use http_body_util::{BodyExt, Empty};
    use hyper::body::Bytes;
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpSocket;

    use super::*;
    use crate::tailscale::UserId;
    use crate::tailscale::fake::{Fake, device};

    const OWNER: UserId = UserId::new(1);
    const OTHER: UserId = UserId::new(2);

    fn fake() -> Fake {
        let mut fake = Fake::new(device("me", OWNER, "127.0.0.1"));
        fake.add_device(device("laptop", OWNER, "127.0.0.2"));
        fake.add_device(device("friend", OTHER, "127.0.0.3"));
        let mut tagged = device("server", OWNER, "127.0.0.4");
        tagged.tagged = true;
        fake.add_device(tagged);
        fake.fail_whois_for("127.0.0.5".parse().unwrap());
        fake
    }

    /// Serves a route that answers with the caller's name.
    async fn start(fake: Fake) -> SocketAddr {
        let authorizer = Arc::new(Authorizer::new(fake).await.unwrap());
        let router = Router::new().route(
            "/",
            get(|Extension(caller): Extension<Identity>| async move { caller.name }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, router, authorizer));
        addr
    }

    /// Sends a GET for `path` to `server` from the address `from`.
    async fn get_from(from: &str, server: SocketAddr, path: &str) -> (StatusCode, String) {
        let socket = TcpSocket::new_v4().unwrap();
        socket
            .bind(SocketAddr::new(from.parse().unwrap(), 0))
            .unwrap();
        let stream = socket.connect(server).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(connection);
        let request = hyper::Request::get(path)
            .header(hyper::header::HOST, "heighliner")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn owners_devices_are_allowed() {
        let server = start(fake()).await;
        assert_eq!(
            get_from("127.0.0.1", server, "/").await,
            (StatusCode::OK, "me".into())
        );
        assert_eq!(
            get_from("127.0.0.2", server, "/").await,
            (StatusCode::OK, "laptop".into())
        );
    }

    #[tokio::test]
    async fn others_are_refused_on_every_route() {
        let server = start(fake()).await;
        for (from, name) in [
            ("127.0.0.3", Some("friend")),
            ("127.0.0.4", Some("server")),
            ("127.0.0.6", None),
        ] {
            for path in ["/", "/no-such-route"] {
                let (status, body) = get_from(from, server, path).await;
                assert_eq!(status, StatusCode::FORBIDDEN, "{from} {path}");
                if let Some(name) = name {
                    assert!(body.contains(name), "{body}");
                }
            }
        }
    }

    #[tokio::test]
    async fn failed_lookups_refuse_the_request_without_details() {
        let server = start(fake()).await;
        let (status, body) = get_from("127.0.0.5", server, "/").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            "can't identify the caller right now; see heighliner's log"
        );
    }

    #[tokio::test]
    async fn tagging_this_device_while_running_refuses_requests() {
        let fake = fake();
        let authorizer = Authorizer::new(fake.clone()).await.unwrap();
        let laptop = "127.0.0.2:1".parse().unwrap();
        assert!(authorizer.authorize(laptop).await.is_ok());

        let mut me = device("me", OWNER, "127.0.0.1");
        me.tagged = true;
        fake.set_me(me);
        assert!(matches!(
            authorizer.authorize(laptop).await,
            Err(Denied::OwnerChanged)
        ));
    }

    /// Connects to `server` from `from`.
    async fn connect_from(from: &str, server: SocketAddr) -> TcpStream {
        let socket = TcpSocket::new_v4().unwrap();
        socket
            .bind(SocketAddr::new(from.parse().unwrap(), 0))
            .unwrap();
        socket.connect(server).await.unwrap()
    }

    #[tokio::test]
    async fn strangers_are_answered_before_they_send_anything() {
        let server = start(fake()).await;
        let mut stream = connect_from("127.0.0.6", server).await;
        let mut answer = String::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_string(&mut answer))
            .await
            .unwrap()
            .unwrap();
        assert!(answer.starts_with("HTTP/1.1 403 Forbidden\r\n"), "{answer}");
        assert!(answer.contains("is not on the tailnet"), "{answer}");
    }

    #[tokio::test]
    async fn idle_connections_are_closed() {
        let authorizer = Arc::new(Authorizer::new(fake()).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = listener.local_addr().unwrap();
        tokio::spawn(serve_with(
            listener,
            Router::new(),
            authorizer,
            Duration::from_millis(200),
        ));
        let mut stream = connect_from("127.0.0.2", server).await;
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut rest))
            .await
            .expect("the server closes an idle connection")
            .unwrap();
    }

    #[tokio::test]
    async fn a_tagged_node_refuses_to_start() {
        let mut me = device("me", OWNER, "127.0.0.1");
        me.tagged = true;
        assert!(matches!(
            Authorizer::new(Fake::new(me)).await,
            Err(tailscale::Error::Tagged)
        ));
    }
}
