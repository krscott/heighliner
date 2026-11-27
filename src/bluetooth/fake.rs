//! A fake Bluetooth stack for tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use super::{Address, Bluetooth, Error, Paired};

#[derive(Default)]
struct State {
    adapter: bool,
    paired: BTreeMap<Address, Paired>,
    /// Peripherals in pairing mode, with their own names.
    pairable: BTreeMap<Address, String>,
    /// Peripherals that fail to connect.
    unreachable: BTreeSet<Address>,
    /// What was asked, in order, such as `connect 98:…`.
    log: Vec<String>,
}

/// Clones share their state, so a test can change it after handing the
/// fake over.
#[derive(Clone, Default)]
pub struct Fake {
    state: Arc<Mutex<State>>,
}

/// A paired Peripheral at `address`, named `alias` here.
pub(crate) fn paired(address: &str, alias: &str, connected: bool) -> Paired {
    Paired {
        address: address.parse().unwrap(),
        alias: alias.to_owned(),
        connected,
    }
}

impl Fake {
    /// A stack with an adapter, and `paired` paired with it.
    pub(crate) fn with(paired: impl IntoIterator<Item = Paired>) -> Self {
        let fake = Self::default();
        {
            let mut state = fake.state.lock().unwrap();
            state.adapter = true;
            state.paired = paired
                .into_iter()
                .map(|paired| (paired.address.clone(), paired))
                .collect();
        }
        fake
    }

    pub(crate) fn bluetooth(&self) -> Bluetooth {
        Bluetooth::Fake(self.clone())
    }

    /// Puts `address` in pairing mode, with `name` as its own name.
    pub(crate) fn make_pairable(&self, address: &str, name: &str) {
        let mut state = self.state.lock().unwrap();
        state
            .pairable
            .insert(address.parse().unwrap(), name.to_owned());
    }

    /// Makes `address` fail to connect.
    pub(crate) fn make_unreachable(&self, address: &str) {
        let mut state = self.state.lock().unwrap();
        state.unreachable.insert(address.parse().unwrap());
    }

    pub(crate) fn get(&self, address: &str) -> Option<Paired> {
        let address: Address = address.parse().unwrap();
        self.state.lock().unwrap().paired.get(&address).cloned()
    }

    pub(crate) fn log(&self) -> Vec<String> {
        self.state.lock().unwrap().log.clone()
    }

    /// Runs `op` on the paired Peripheral at `address`, logging `what`.
    fn with_paired<R>(
        &self,
        what: &str,
        address: &Address,
        op: impl FnOnce(&mut Paired, &BTreeSet<Address>) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut state = self.state.lock().unwrap();
        state.log.push(format!("{what} {address}"));
        if !state.adapter {
            return Err(Error::NoAdapter);
        }
        let State {
            paired,
            unreachable,
            ..
        } = &mut *state;
        let peripheral = paired.get_mut(address).ok_or(Error::NotPaired)?;
        op(peripheral, unreachable)
    }

    pub(super) fn paired(&self) -> Result<Option<Vec<Paired>>, Error> {
        let state = self.state.lock().unwrap();
        Ok(state
            .adapter
            .then(|| state.paired.values().cloned().collect()))
    }

    pub(super) fn connect(&self, address: &Address) -> Result<(), Error> {
        self.with_paired("connect", address, |peripheral, unreachable| {
            if unreachable.contains(address) {
                return Err(Error::Bluez {
                    name: "org.bluez.Error.Failed".into(),
                    message: Some("br-connection-page-timeout".into()),
                });
            }
            peripheral.connected = true;
            Ok(())
        })
    }

    pub(super) fn disconnect(&self, address: &Address) -> Result<(), Error> {
        self.with_paired("disconnect", address, |peripheral, _| {
            peripheral.connected = false;
            Ok(())
        })
    }

    pub(super) fn set_alias(&self, address: &Address, alias: &str) -> Result<(), Error> {
        self.with_paired(&format!("name {alias:?}"), address, |peripheral, _| {
            peripheral.alias = alias.to_owned();
            Ok(())
        })
    }

    pub(super) fn forget(&self, address: &Address) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        state.log.push(format!("forget {address}"));
        state
            .paired
            .remove(address)
            .map(drop)
            .ok_or(Error::NotPaired)
    }

    pub(super) fn pair(&self, address: &Address) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        state.log.push(format!("pair {address}"));
        if !state.adapter {
            return Err(Error::NoAdapter);
        }
        if state.paired.contains_key(address) {
            return Err(Error::AlreadyPaired);
        }
        let name = state.pairable.remove(address).ok_or(Error::NotSeen)?;
        state.paired.insert(
            address.clone(),
            Paired {
                address: address.clone(),
                alias: name,
                connected: false,
            },
        );
        Ok(())
    }
}
