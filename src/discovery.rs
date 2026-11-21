//! Finding the devices that run heighliner.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::task::JoinSet;

use crate::protocol::{self, Hello};
use crate::tailscale::{Device, Tailnet};

/// A device that answered on heighliner's port, the address it answered
/// on, and its answer to hello, which can be an error, such as a refusal.
#[derive(Debug)]
pub struct Found {
    pub device: Device,
    pub addr: SocketAddr,
    pub hello: Result<Hello, protocol::Error>,
}

/// Asks the devices owned by this device's owner for the Node protocol's
/// hello on `port`, and returns the ones that answered. Each device's
/// addresses are tried in turn until one answers, each for at most
/// `timeout`; devices are asked concurrently.
pub async fn discover(tailnet: &Tailnet, port: u16, timeout: Duration) -> Vec<Found> {
    let mut probes = JoinSet::new();
    for device in tailnet.owned_devices().cloned() {
        probes.spawn(async move {
            for ip in &device.ips {
                let addr = SocketAddr::new(*ip, port);
                let hello = protocol::hello(addr, timeout).await;
                if !hello.as_ref().is_err_and(protocol::Error::is_unreachable) {
                    return Some(Found {
                        device,
                        addr,
                        hello,
                    });
                }
            }
            None
        });
    }
    let mut found: Vec<Found> = probes.join_all().await.into_iter().flatten().collect();
    found.sort_by(|a, b| a.device.id.cmp(&b.device.id));
    found
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::routing::get;
    use tokio::net::TcpListener;

    use super::*;
    use crate::tailscale::UserId;
    use crate::tailscale::fake::device;

    const OWNER: UserId = UserId::new(1);
    const OTHER: UserId = UserId::new(2);

    #[tokio::test]
    async fn finds_owned_devices_that_say_hello() {
        let hello = Router::new().route(
            protocol::HELLO_PATH,
            get(|| async { axum::Json(Hello::ours()) }),
        );
        // Answers, but refuses.
        let refusing = Router::new().route(
            protocol::HELLO_PATH,
            get(|| async { (axum::http::StatusCode::FORBIDDEN, "no") }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, hello).await });
        let listener = TcpListener::bind(("127.0.0.5", port)).await.unwrap();
        tokio::spawn(async move { axum::serve(listener, refusing).await });
        // Accepts connections on the same port, but never answers.
        let silent = TcpListener::bind(("127.0.0.4", port)).await.unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = silent.accept().await {
                held.push(stream);
            }
        });

        // Answers, on its second address.
        let mut answering = device("answering", OWNER, "127.0.0.2");
        answering.ips.push("127.0.0.1".parse().unwrap());
        let mut tagged = device("tagged", OWNER, "127.0.0.1");
        tagged.tagged = true;
        let tailnet = Tailnet::new(
            device("me", OWNER, "100.64.0.1"),
            vec![
                answering,
                // Owned, but nothing listens.
                device("idle", OWNER, "127.0.0.3"),
                // Listens, but never answers.
                device("silent", OWNER, "127.0.0.4"),
                device("refusing", OWNER, "127.0.0.5"),
                // Answers, but isn't the owner's.
                device("friend", OTHER, "127.0.0.1"),
                tagged,
            ],
        )
        .unwrap();

        let found = discover(&tailnet, port, Duration::from_millis(500)).await;
        let names: Vec<_> = found.iter().map(|f| f.device.name.as_str()).collect();
        assert_eq!(names, ["answering", "refusing"]);
        assert_eq!(
            found[0].addr,
            SocketAddr::new("127.0.0.1".parse().unwrap(), port)
        );
        assert_eq!(found[0].hello.as_ref().unwrap(), &Hello::ours());
        assert!(matches!(
            found[1].hello,
            Err(protocol::Error::Status { .. })
        ));
    }
}
