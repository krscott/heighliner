//! A fake local API for tests.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use super::{Device, Error, Identity, LocalApi, Tailnet, UserId};

/// What status reports.
struct Devices {
    me: Device,
    others: Vec<Device>,
}
use crate::model::NodeId;

/// An untagged device named `name`, with node ID `n-<name>`, at `ip`.
pub(crate) fn device(name: &str, user: UserId, ip: &str) -> Device {
    Device {
        id: NodeId::new(format!("n-{name}")).unwrap(),
        name: name.to_owned(),
        user,
        tagged: false,
        ips: vec![ip.parse().unwrap()],
    }
}

/// Clones share their status, so a test can change it after handing the
/// fake over.
#[derive(Clone)]
pub(crate) struct Fake {
    devices: Arc<Mutex<Devices>>,
    identities: BTreeMap<IpAddr, Identity>,
    failing_whois: BTreeSet<IpAddr>,
}

impl Fake {
    pub(crate) fn new(me: Device) -> Self {
        let mut fake = Self {
            devices: Arc::new(Mutex::new(Devices {
                me: me.clone(),
                others: Vec::new(),
            })),
            identities: BTreeMap::new(),
            failing_whois: BTreeSet::new(),
        };
        fake.add_identities(&me);
        fake
    }

    /// Adds another device, which WhoIs then identifies.
    pub(crate) fn add_device(&mut self, device: Device) {
        self.add_identities(&device);
        self.devices.lock().unwrap().others.push(device);
    }

    /// Changes what status reports about this device.
    pub(crate) fn set_me(&self, me: Device) {
        self.devices.lock().unwrap().me = me;
    }

    pub(crate) fn fail_whois_for(&mut self, ip: IpAddr) {
        self.failing_whois.insert(ip);
    }

    fn add_identities(&mut self, device: &Device) {
        for ip in &device.ips {
            self.identities.insert(
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

    async fn whois(&self, addr: SocketAddr) -> Result<Option<Identity>, Error> {
        if self.failing_whois.contains(&addr.ip()) {
            return Err(Error::Timeout {
                path: "/localapi/v0/whois".to_owned(),
            });
        }
        Ok(self.identities.get(&addr.ip()).cloned())
    }
}
