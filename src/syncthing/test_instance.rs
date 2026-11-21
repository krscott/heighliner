//! A real Syncthing instance for tests.

use std::fs;
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::Value;
use tempfile::TempDir;

use super::{Client, Driver, Report};
use crate::config::Config;
use crate::daemon::RETENTION_WINDOW;
use crate::model::{AbsPath, ItemId};
use crate::tailscale::Tailnet;

const API_KEY: &str = "test-api-key";

/// A Syncthing instance in a temporary home, stopped when dropped.
pub(crate) struct Syncthing {
    pub(crate) home: TempDir,
    child: Child,
    pub(crate) driver: Driver,
}

impl Syncthing {
    /// Starts Syncthing listening on `ip`:`port`. Its first configuration
    /// is hardened before it starts, so it never contacts the internet.
    pub(crate) async fn start(ip: &str, port: u16) -> Self {
        let home = tempfile::tempdir().unwrap();
        let generate = Command::new("syncthing")
            .arg("generate")
            .arg(format!("--home={}", home.path().display()))
            .arg("--no-port-probing")
            .env("STNODEFAULTFOLDER", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("syncthing is on PATH; run the tests in `nix develop`");
        assert!(generate.success());
        let config_file = home.path().join("config.xml");
        let mut config = fs::read_to_string(&config_file).unwrap();
        for (from, to) in [
            (
                "<globalAnnounceEnabled>true",
                "<globalAnnounceEnabled>false",
            ),
            ("<localAnnounceEnabled>true", "<localAnnounceEnabled>false"),
            ("<relaysEnabled>true", "<relaysEnabled>false"),
            ("<natEnabled>true", "<natEnabled>false"),
            (
                "<crashReportingEnabled>true",
                "<crashReportingEnabled>false",
            ),
            ("<urAccepted>0", "<urAccepted>-1"),
            ("<stunKeepaliveStartS>180", "<stunKeepaliveStartS>0"),
            (
                "<listenAddress>default",
                &format!("<listenAddress>tcp://{ip}:{port}"),
            ),
        ] {
            assert!(config.contains(from), "{from}");
            config = config.replace(from, to);
        }
        fs::write(&config_file, config).unwrap();

        let socket = home.path().join("gui.sock");
        let child = Command::new("syncthing")
            .arg("serve")
            .arg(format!("--home={}", home.path().display()))
            .arg(format!("--gui-address=unix://{}", socket.display()))
            .arg(format!("--gui-apikey={API_KEY}"))
            .arg(format!("--log-file={}", home.path().join("log").display()))
            .args(["--no-browser", "--no-upgrade"])
            // Syncthing runs a monitor process and a child; a process
            // group of their own lets Drop stop both.
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let driver = Driver::with_port(
            Client::new(&socket, API_KEY).unwrap(),
            port,
            RETENTION_WINDOW,
        );
        for _ in 0..100 {
            if driver.client().device_id().await.is_ok() {
                return Self {
                    home,
                    child,
                    driver,
                };
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        kill_group(&child);
        let mut child = child;
        let _ = child.wait();
        panic!("Syncthing didn't start");
    }

    /// Reconciles as the daemon does.
    pub(crate) async fn reconcile(&self, config: &Config, tailnet: &Tailnet) -> Report {
        let device_id = self.driver.client().device_id().await.unwrap();
        self.driver
            .reconcile(config, tailnet, &device_id)
            .await
            .unwrap()
    }

    pub(crate) async fn config(&self) -> Value {
        self.driver.client().get("/rest/config").await.unwrap()
    }

    pub(crate) async fn folder(&self, item: ItemId) -> Option<Value> {
        self.config().await["folders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|folder| folder["id"] == item.to_string())
            .cloned()
    }

    pub(crate) fn dir(&self, name: &str) -> AbsPath {
        AbsPath::new(self.home.path().join(name).to_str().unwrap()).unwrap()
    }
}

impl Drop for Syncthing {
    fn drop(&mut self) {
        kill_group(&self.child);
        let _ = self.child.wait();
    }
}

/// A port free on both loopback addresses the tests use.
pub(crate) fn free_port() -> u16 {
    loop {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = first.local_addr().unwrap().port();
        if TcpListener::bind(("127.0.0.2", port)).is_ok() {
            return port;
        }
    }
}

/// Stops Syncthing's monitor and child, which share `child`'s process group.
fn kill_group(child: &Child) {
    let group = format!("-{}", child.id());
    let _ = Command::new("kill").args(["-KILL", "--", &group]).status();
}
