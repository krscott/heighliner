//! Finding the devices that run heighliner.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::task::JoinSet;

use crate::protocol::{self, Hello};
use crate::tailscale::{Device, Tailnet};

/// A device that answered the Node protocol's hello, the address it
/// answered on, and what it said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub device: Device,
    pub addr: SocketAddr,
    pub hello: Hello,
}

/// Finds the devices owned by this device's owner that answer the Node
/// protocol's hello on `port`. Each device's addresses are tried in turn,
/// each for at most `timeout`; devices are asked concurrently.
pub async fn discover(tailnet: &Tailnet, port: u16, timeout: Duration) -> Vec<Found> {
    let mut probes = JoinSet::new();
    for device in tailnet.owned_devices().cloned() {
        probes.spawn(async move {
            for ip in &device.ips {
                let addr = SocketAddr::new(*ip, port);
                if let Ok(hello) = protocol::hello(addr, timeout).await {
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, hello).await });
        // Accepts connections on the same port, but isn't heighliner.
        let silent = TcpListener::bind(("127.0.0.4", port)).await.unwrap();
        tokio::spawn(async move {
            loop {
                let _ = silent.accept().await;
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
                // Listens, but doesn't speak the protocol.
                device("silent", OWNER, "127.0.0.4"),
                // Answers, but isn't the owner's.
                device("friend", OTHER, "127.0.0.1"),
                tagged,
            ],
        )
        .unwrap();

        let found = discover(&tailnet, port, Duration::from_millis(500)).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].device.name, "answering");
        assert_eq!(
            found[0].addr,
            SocketAddr::new("127.0.0.1".parse().unwrap(), port)
        );
        assert_eq!(found[0].hello, Hello::ours());
    }
}
