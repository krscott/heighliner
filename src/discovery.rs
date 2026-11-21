//! Finding the devices that run heighliner.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::task::JoinSet;

use crate::tailscale::{Device, Tailnet};

/// A device that accepted a connection, and the address it accepted it on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub device: Device,
    pub addr: SocketAddr,
}

/// Finds the devices owned by this device's owner that accept TCP
/// connections on `port`, the port heighliner listens on. Each device's
/// addresses are tried in turn, each for at most `timeout`; devices are
/// probed concurrently.
pub async fn discover(tailnet: &Tailnet, port: u16, timeout: Duration) -> Vec<Found> {
    let mut probes = JoinSet::new();
    for device in tailnet.owned_devices().cloned() {
        probes.spawn(async move {
            for ip in &device.ips {
                let addr = SocketAddr::new(*ip, port);
                if let Ok(Ok(_)) = tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
                    return Some(Found { device, addr });
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
    use tokio::net::TcpListener;

    use super::*;
    use crate::tailscale::UserId;
    use crate::tailscale::fake::device;

    const OWNER: UserId = UserId::new(1);
    const OTHER: UserId = UserId::new(2);

    #[tokio::test]
    async fn finds_owned_devices_that_listen() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });

        // Listens, on its second address.
        let mut listening = device("listening", OWNER, "127.0.0.2");
        listening.ips.push("127.0.0.1".parse().unwrap());
        let mut tagged = device("tagged", OWNER, "127.0.0.1");
        tagged.tagged = true;
        let tailnet = Tailnet::new(
            device("me", OWNER, "100.64.0.1"),
            vec![
                listening,
                // Owned, but nothing listens.
                device("idle", OWNER, "127.0.0.3"),
                // Listens, but isn't the owner's.
                device("friend", OTHER, "127.0.0.1"),
                tagged,
            ],
        )
        .unwrap();

        let found = discover(&tailnet, port, Duration::from_secs(1)).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].device.name, "listening");
        assert_eq!(
            found[0].addr,
            SocketAddr::new("127.0.0.1".parse().unwrap(), port)
        );
    }

}
