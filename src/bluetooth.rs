//! This Node's Bluetooth stack, as BlueZ exposes it on the system bus: the
//! Peripherals paired with it, and connecting, disconnecting, pairing,
//! forgetting and naming them.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

mod bluez;
#[cfg(test)]
pub(crate) mod fake;

pub use bluez::{Bluez, SCAN};

/// A Peripheral's Bluetooth address, as six uppercase hex pairs joined by
/// colons, which is how BlueZ writes it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Address(String);

impl Address {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Why a string isn't a Bluetooth address.
#[derive(Debug)]
pub struct NotAnAddress(String);

impl fmt::Display for NotAnAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} isn't a Bluetooth address such as 98:47:44:A6:21:19",
            self.0
        )
    }
}

impl std::error::Error for NotAnAddress {}

impl FromStr for Address {
    type Err = NotAnAddress;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let valid = parts.len() == 6
            && parts
                .iter()
                .all(|part| part.len() == 2 && part.bytes().all(|byte| byte.is_ascii_hexdigit()));
        if valid {
            Ok(Self(s.to_ascii_uppercase()))
        } else {
            Err(NotAnAddress(s.to_owned()))
        }
    }
}

impl TryFrom<String> for Address {
    type Error = NotAnAddress;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Address> for String {
    fn from(address: Address) -> Self {
        address.0
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A Peripheral paired with this Node, as its Bluetooth stack sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Paired {
    pub address: Address,
    /// What this Node calls it: the name it was given here, or its own.
    pub alias: String,
    pub connected: bool,
}

/// Why a Bluetooth operation failed, as seen from the Node it ran on.
#[derive(Debug)]
pub enum Error {
    /// BlueZ isn't running, or manages no adapter.
    NoAdapter,
    NotPaired,
    AlreadyPaired,
    /// It didn't show up while this Node scanned for it.
    NotSeen,
    /// Pairing it needs a PIN or passkey.
    NeedsCode,
    /// This Node is already pairing a Peripheral.
    Busy,
    /// BlueZ didn't answer in time.
    Timeout,
    /// BlueZ refused, with its error's name and message.
    Bluez {
        name: String,
        message: Option<String>,
    },
    /// Talking to the system bus failed.
    Bus(zbus::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAdapter => f.write_str("it has no Bluetooth adapter, or BlueZ isn't running"),
            Self::NotPaired => f.write_str("the Peripheral isn't paired with it"),
            Self::AlreadyPaired => f.write_str("the Peripheral is already paired with it"),
            Self::NotSeen => write!(
                f,
                "it didn't see the Peripheral within {} s; is the Peripheral in pairing mode?",
                SCAN.as_secs()
            ),
            Self::NeedsCode => f.write_str(
                "the Peripheral asks for a PIN or passkey, which heighliner can't enter",
            ),
            Self::Busy => {
                f.write_str("it is already pairing a Peripheral; try again once that's done")
            }
            Self::Timeout => f.write_str("BlueZ didn't answer in time"),
            Self::Bluez { name, message } => {
                let message = message.as_deref().unwrap_or(name);
                write!(f, "BlueZ says {message}")?;
                if message.contains("page-timeout") {
                    f.write_str("; is the Peripheral on and in range?")?;
                }
                Ok(())
            }
            Self::Bus(error) => write!(f, "can't talk to BlueZ: {error}"),
        }
    }
}

impl std::error::Error for Error {}

/// This Node's Bluetooth stack.
#[derive(Clone)]
pub enum Bluetooth {
    Bluez(Bluez),
    #[cfg(test)]
    Fake(fake::Fake),
}

impl Bluetooth {
    /// The Peripherals paired with this Node, or `None` if it has no
    /// adapter.
    pub async fn paired(&self) -> Result<Option<Vec<Paired>>, Error> {
        match self {
            Self::Bluez(bluez) => bluez.paired().await,
            #[cfg(test)]
            Self::Fake(fake) => fake.paired(),
        }
    }

    /// Connects `address`, which must be paired, powering the adapter on
    /// first and retrying while the Peripheral frees itself from another
    /// host.
    pub async fn connect(&self, address: &Address) -> Result<(), Error> {
        match self {
            Self::Bluez(bluez) => bluez.connect(address).await,
            #[cfg(test)]
            Self::Fake(fake) => fake.connect(address),
        }
    }

    /// Disconnects `address`, which must be paired; one that isn't
    /// connected stays so.
    pub async fn disconnect(&self, address: &Address) -> Result<(), Error> {
        match self {
            Self::Bluez(bluez) => bluez.disconnect(address).await,
            #[cfg(test)]
            Self::Fake(fake) => fake.disconnect(address),
        }
    }

    /// Scans for `address`, which must be in pairing mode, then pairs and
    /// trusts it.
    pub async fn pair(&self, address: &Address) -> Result<(), Error> {
        match self {
            Self::Bluez(bluez) => bluez.pair(address).await,
            #[cfg(test)]
            Self::Fake(fake) => fake.pair(address),
        }
    }

    /// Unpairs `address`.
    pub async fn forget(&self, address: &Address) -> Result<(), Error> {
        match self {
            Self::Bluez(bluez) => bluez.forget(address).await,
            #[cfg(test)]
            Self::Fake(fake) => fake.forget(address),
        }
    }

    /// Names `address`, which must be paired, `alias` on this Node.
    pub async fn set_alias(&self, address: &Address, alias: &str) -> Result<(), Error> {
        match self {
            Self::Bluez(bluez) => bluez.set_alias(address, alias).await,
            #[cfg(test)]
            Self::Fake(fake) => fake.set_alias(address, alias),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_are_six_hex_pairs_kept_uppercase() {
        let address: Address = "98:47:44:a6:21:19".parse().unwrap();
        assert_eq!(address.as_str(), "98:47:44:A6:21:19");
        for odd in [
            "",
            "98:47:44:A6:21",
            "98:47:44:A6:21:19:00",
            "98-47-44-A6-21-19",
            "98:47:44:A6:21:1G",
            "984:7:44:A6:21:19",
        ] {
            assert!(odd.parse::<Address>().is_err(), "{odd}");
        }
        let json = serde_json::to_string(&address).unwrap();
        assert_eq!(json, "\"98:47:44:A6:21:19\"");
        assert!(serde_json::from_str::<Address>("\"nope\"").is_err());
    }

    #[test]
    fn a_page_timeout_suggests_why() {
        let error = Error::Bluez {
            name: "org.bluez.Error.Failed".into(),
            message: Some("br-connection-page-timeout".into()),
        };
        assert_eq!(
            error.to_string(),
            "BlueZ says br-connection-page-timeout; is the Peripheral on and in range?"
        );
    }
}
