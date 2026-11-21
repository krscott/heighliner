//! Authorization: every request, from another Node, the CLI or the web UI,
//! must come from a device owned by the user who owns this Node.
//!
//! Tailscale's WhoIs identifies the device behind a connection, so this
//! needs no keys of its own. Tagged devices belong to Tailscale's
//! tagged-devices user, so they are refused like any other user's.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::tailscale::{self, Identity, LocalApi, UserId};

/// Checks callers against the user who owns this Node.
pub struct Authorizer<T> {
    api: T,
    owner: UserId,
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
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotOnTailnet(addr) => write!(f, "{addr} is not on the tailnet"),
            Self::NotOwned(identity) => {
                write!(f, "{} is not owned by this Node's user", identity.name)
            }
            Self::Lookup(error) => write!(f, "can't identify the caller: {error}"),
        }
    }
}

impl IntoResponse for Denied {
    fn into_response(self) -> Response {
        let status = match self {
            Self::NotOnTailnet(_) | Self::NotOwned(_) => StatusCode::FORBIDDEN,
            Self::Lookup(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, self.to_string()).into_response()
    }
}

impl<T: LocalApi> Authorizer<T> {
    /// Reads this Node's owner from `api`.
    pub async fn new(api: T) -> Result<Self, tailscale::Error> {
        let owner = api.status().await?.me.user;
        Ok(Self { api, owner })
    }

    pub fn owner(&self) -> UserId {
        self.owner
    }

    /// Identifies the caller at `addr`, if it may make requests.
    pub async fn authorize(&self, addr: SocketAddr) -> Result<Identity, Denied> {
        match self.api.whois(addr).await {
            Ok(Some(identity)) if identity.user == self.owner => Ok(identity),
            Ok(Some(identity)) => Err(Denied::NotOwned(identity)),
            Ok(None) => Err(Denied::NotOnTailnet(addr)),
            Err(error) => Err(Denied::Lookup(error)),
        }
    }
}

/// Axum middleware that refuses requests from callers [`Authorizer`]
/// refuses, and gives handlers the caller's [`Identity`] as a request
/// extension.
///
/// The server must provide `ConnectInfo<SocketAddr>`, as
/// `into_make_service_with_connect_info::<SocketAddr>()` does; without it,
/// every request fails.
pub async fn require_owner<T: LocalApi>(
    State(authorizer): State<Arc<Authorizer<T>>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    mut request: Request,
    next: Next,
) -> Response {
    match authorizer.authorize(addr).await {
        Ok(identity) => {
            request.extensions_mut().insert(identity);
            next.run(request).await
        }
        Err(denied) => denied.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::Extension;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::routing::get;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::tailscale::fake::{Fake, device, identity};

    const OWNER: UserId = UserId::new(1);
    const OTHER: UserId = UserId::new(2);

    fn fake() -> Fake {
        let mut fake = Fake::new(device("me", OWNER, "100.64.0.1"));
        fake.add_peer(device("laptop", OWNER, "100.64.0.2"));
        fake.add_peer(device("friend", OTHER, "100.64.0.3"));
        fake
    }

    async fn call(fake: Fake, from: &str) -> (StatusCode, String) {
        let authorizer = Arc::new(Authorizer::new(fake).await.unwrap());
        let app = Router::new()
            .route(
                "/",
                get(|Extension(caller): Extension<Identity>| async move { caller.name }),
            )
            .layer(axum::middleware::from_fn_with_state(
                authorizer,
                require_owner,
            ))
            .layer(MockConnectInfo(SocketAddr::new(
                from.parse::<IpAddr>().unwrap(),
                40000,
            )));
        let response = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn owners_devices_are_allowed() {
        assert_eq!(
            call(fake(), "100.64.0.2").await,
            (StatusCode::OK, "laptop".into())
        );
        assert_eq!(
            call(fake(), "100.64.0.1").await,
            (StatusCode::OK, "me".into())
        );
    }

    #[tokio::test]
    async fn other_users_and_strangers_are_refused() {
        let (status, body) = call(fake(), "100.64.0.3").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body.contains("friend"), "{body}");

        let (status, _) = call(fake(), "192.0.2.1").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn failed_lookups_refuse_the_request() {
        let mut fake = fake();
        fake.add_identity("100.64.0.9".parse().unwrap(), identity("broken", OWNER));
        fake.fail_whois_for("100.64.0.9".parse().unwrap());
        let (status, body) = call(fake, "100.64.0.9").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("can't identify"), "{body}");
    }
}
