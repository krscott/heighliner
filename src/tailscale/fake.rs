//! A fake local API for tests.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use super::{Device, Error, Identity, LocalApi, Tailnet, UserId};
use crate::model::NodeId;

/// What status reports.
struct Devices {
    me: Device,
    others: Vec<Device>,
}

/// An untagged device named `name`, with node ID `n-<name>`, at `ip`.
pub(crate) fn device(name: &str, user: UserId, ip: &str) -> Device {
    Device {
        id: NodeId::new(format!("n-{name}")).unwrap(),
        name: name.to_owned(),
        dns_name: format!("{name}.tail.ts.net"),
        user,
        tagged: false,
        ips: vec![ip.parse().unwrap()],
        caches_network_maps: true,
    }
}

/// Clones share their status, so a test can change it after handing the
/// fake over.
#[derive(Clone)]
pub(crate) struct Fake {
    devices: Arc<Mutex<Devices>>,
    identities: Arc<Mutex<BTreeMap<IpAddr, Identity>>>,
    failing_whois: Arc<Mutex<BTreeSet<IpAddr>>>,
}

impl Fake {
    pub(crate) fn new(me: Device) -> Self {
        let fake = Self {
            devices: Arc::new(Mutex::new(Devices {
                me: me.clone(),
                others: Vec::new(),
            })),
            identities: Arc::default(),
            failing_whois: Arc::default(),
        };
        fake.add_identities(&me);
        fake
    }

    /// Adds another device, or changes one at the same addresses.
    pub(crate) fn add_device(&self, device: Device) {
        self.add_identities(&device);
        self.devices.lock().unwrap().others.push(device);
    }

    /// Changes what status reports about this device.
    pub(crate) fn set_me(&self, me: Device) {
        self.devices.lock().unwrap().me = me;
    }

    pub(crate) fn fail_whois_for(&self, ip: IpAddr) {
        self.failing_whois.lock().unwrap().insert(ip);
    }

    fn add_identities(&self, device: &Device) {
        let mut identities = self.identities.lock().unwrap();
        for ip in &device.ips {
            identities.insert(
                *ip,
                Identity {
                    node: device.id.clone(),
                    name: device.name.clone(),
                    user: device.user,
                    tagged: device.tagged,
                },
            );
        }
    }
}

impl LocalApi for Fake {
    async fn status(&self) -> Result<Tailnet, Error> {
        let devices = self.devices.lock().unwrap();
        Tailnet::new(devices.me.clone(), devices.others.clone())
    }

    async fn this_device(&self) -> Result<Device, Error> {
        Ok(self.devices.lock().unwrap().me.clone())
    }

    async fn whois(&self, addr: SocketAddr) -> Result<Option<Identity>, Error> {
        if self.failing_whois.lock().unwrap().contains(&addr.ip()) {
            return Err(Error::Timeout {
                path: "/localapi/v0/whois".to_owned(),
            });
        }
        Ok(self.identities.lock().unwrap().get(&addr.ip()).cloned())
    }
}
