//! BlueZ on the system bus, which lets any user manage its devices.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::time::Instant;
use zbus::fdo::ObjectManagerProxy;
use zbus::message::Type;
use zbus::names::OwnedInterfaceName;
use zbus::proxy::CacheProperties;
use zbus::{Connection, MatchRule, MessageStream, ObjectPath, OwnedObjectPath, OwnedVariant};

use super::{Address, Error, Paired};

const SERVICE: &str = "org.bluez";
const ADAPTER: &str = "org.bluez.Adapter1";
const DEVICE: &str = "org.bluez.Device1";

/// Where the agent that answers BlueZ while pairing is served.
const AGENT_PATH: &str = "/net/heighliner/agent";

/// How long pairing scans for the Peripheral.
pub const SCAN: Duration = Duration::from_secs(30);

/// How often a scan checks whether the Peripheral has shown up.
const SCAN_POLL: Duration = Duration::from_millis(500);

/// How long BlueZ may take to answer a call, such as one to pair or
/// connect, which can wait on the Peripheral.
const CALL_TIMEOUT: Duration = Duration::from_secs(25);

/// The properties whose changes the UI shows.
const SHOWN: [&str; 4] = ["Alias", "Connected", "Paired", "Powered"];

/// How long connecting keeps trying while the Peripheral frees itself
/// from the host it just left.
const CONNECT_RETRIES: Duration = Duration::from_secs(15);

const CONNECT_RETRY_DELAY: Duration = Duration::from_secs(1);

#[zbus::proxy(interface = "org.bluez.Adapter1", default_service = "org.bluez")]
trait Adapter1 {
    fn start_discovery(&self) -> zbus::Result<()>;
    fn stop_discovery(&self) -> zbus::Result<()>;
    fn remove_device(&self, device: &ObjectPath<'_>) -> zbus::Result<()>;
    #[zbus(property)]
    fn set_powered(&self, powered: bool) -> zbus::Result<()>;
}

#[zbus::proxy(interface = "org.bluez.Device1", default_service = "org.bluez")]
trait Device1 {
    fn connect(&self) -> zbus::Result<()>;
    fn disconnect(&self) -> zbus::Result<()>;
    fn pair(&self) -> zbus::Result<()>;
    #[zbus(property)]
    fn set_alias(&self, alias: &str) -> zbus::Result<()>;
    #[zbus(property)]
    fn set_trusted(&self, trusted: bool) -> zbus::Result<()>;
}

#[zbus::proxy(
    interface = "org.bluez.AgentManager1",
    default_service = "org.bluez",
    default_path = "/org/bluez"
)]
trait AgentManager1 {
    fn register_agent(&self, agent: &ObjectPath<'_>, capability: &str) -> zbus::Result<()>;
    fn unregister_agent(&self, agent: &ObjectPath<'_>) -> zbus::Result<()>;
}

/// The errors the agent answers BlueZ with.
#[derive(Debug, zbus::DBusError)]
#[zbus(prefix = "org.bluez.Error")]
enum AgentError {
    #[zbus(error)]
    ZBus(zbus::Error),
    Rejected(String),
}

/// Answers BlueZ while one Peripheral pairs: it accepts what needs no
/// code, for that Peripheral only, and refuses PINs and passkeys, which
/// no one is there to enter.
struct Agent {
    device: OwnedObjectPath,
    asked_for_code: Arc<AtomicBool>,
}

impl Agent {
    fn accept(&self, device: &ObjectPath<'_>) -> Result<(), AgentError> {
        if *device == *self.device {
            Ok(())
        } else {
            Err(AgentError::Rejected(
                "not the Peripheral being paired".into(),
            ))
        }
    }

    fn refuse_code(&self) -> AgentError {
        self.asked_for_code.store(true, Ordering::Relaxed);
        AgentError::Rejected("heighliner can't enter a PIN or passkey".into())
    }
}

#[zbus::interface(name = "org.bluez.Agent1")]
impl Agent {
    fn release(&self) {}

    fn request_pin_code(&self, _device: ObjectPath<'_>) -> Result<String, AgentError> {
        Err(self.refuse_code())
    }

    fn display_pin_code(&self, _device: ObjectPath<'_>, _pin: &str) -> Result<(), AgentError> {
        Err(self.refuse_code())
    }

    fn request_passkey(&self, _device: ObjectPath<'_>) -> Result<u32, AgentError> {
        Err(self.refuse_code())
    }

    fn display_passkey(&self, _device: ObjectPath<'_>, _passkey: u32, _entered: u16) {
        self.asked_for_code.store(true, Ordering::Relaxed);
    }

    fn request_confirmation(
        &self,
        device: ObjectPath<'_>,
        _passkey: u32,
    ) -> Result<(), AgentError> {
        self.accept(&device)
    }

    fn request_authorization(&self, device: ObjectPath<'_>) -> Result<(), AgentError> {
        self.accept(&device)
    }

    fn authorize_service(&self, device: ObjectPath<'_>, _uuid: &str) -> Result<(), AgentError> {
        self.accept(&device)
    }

    fn cancel(&self) {}
}

/// A connection to the system bus, for BlueZ.
#[derive(Clone)]
pub struct Bluez {
    connection: Connection,
    /// Held while pairing, which scans and serves the agent.
    pairing: Arc<tokio::sync::Mutex<()>>,
}

/// An adapter, as BlueZ last listed it.
struct Adapter {
    path: OwnedObjectPath,
    powered: bool,
}

/// A device BlueZ knows of, paired or only seen.
struct Device {
    path: OwnedObjectPath,
    adapter: OwnedObjectPath,
    paired: bool,
    /// Whether the current scan has seen it, which BlueZ shows by giving
    /// it a signal strength.
    seen: bool,
    peripheral: Paired,
}

/// BlueZ's adapters and devices.
struct Objects {
    adapters: Vec<Adapter>,
    devices: Vec<Device>,
}

type Properties = HashMap<String, OwnedVariant>;

fn string(properties: &Properties, name: &str) -> Option<String> {
    properties
        .get(name)
        .and_then(|value| <&str>::try_from(value).ok())
        .map(str::to_owned)
}

fn boolean(properties: &Properties, name: &str) -> bool {
    properties
        .get(name)
        .and_then(|value| bool::try_from(value).ok())
        .unwrap_or(false)
}

fn bluez_error(error: zbus::Error) -> Error {
    match error {
        zbus::Error::MethodError(name, message, _) => Error::Bluez {
            name: name.to_string(),
            message,
        },
        zbus::Error::FDO(error) => fdo_error(*error),
        error => Error::Bus(error),
    }
}

fn fdo_error(error: zbus::fdo::Error) -> Error {
    match error {
        zbus::fdo::Error::ServiceUnknown(_) | zbus::fdo::Error::NameHasNoOwner(_) => {
            Error::NoAdapter
        }
        zbus::fdo::Error::ZBus(error) => bluez_error(error),
        error => Error::Bus(error.into()),
    }
}

/// Whether `error` is BlueZ saying `name`, as `org.bluez.Error.<name>`.
fn is_bluez(error: &Error, name: &str) -> bool {
    matches!(error, Error::Bluez { name: said, .. } if said.strip_prefix("org.bluez.Error.") == Some(name))
}

impl Objects {
    fn paired(&self, address: &Address) -> Option<&Device> {
        self.devices
            .iter()
            .find(|device| device.paired && device.peripheral.address == *address)
    }

    fn adapter(&self, path: &OwnedObjectPath) -> Option<&Adapter> {
        self.adapters.iter().find(|adapter| adapter.path == *path)
    }
}

impl Bluez {
    /// Connects to the system bus. BlueZ itself needn't be running yet.
    pub async fn open() -> Result<Self, Error> {
        let connection = zbus::connection::Builder::system()
            .method_timeout(CALL_TIMEOUT)
            .build()
            .await
            .map_err(Error::Bus)?;
        Ok(Self {
            connection,
            pairing: Arc::default(),
        })
    }

    /// Calls `changed` whenever BlueZ says something the UI shows changed,
    /// such as a Peripheral connecting, until the connection to the bus
    /// closes. Changes a scan makes, such as signal strengths, don't count.
    pub async fn watch(&self, changed: impl Fn()) -> Result<(), Error> {
        let rule = MatchRule::builder()
            .msg_type(Type::Signal)
            .sender(SERVICE)
            .build()
            .map_err(Error::Bus)?;
        let mut signals = MessageStream::for_match_rule(rule, &self.connection, None)
            .await
            .map_err(Error::Bus)?;
        while let Some(signal) = signals.next().await {
            if signal.is_ok_and(|signal| shown(&signal)) {
                changed();
            }
        }
        Ok(())
    }

    async fn objects(&self) -> Result<Objects, Error> {
        let manager = ObjectManagerProxy::builder(&self.connection)
            .destination(SERVICE)
            .path("/")
            .cache_properties(CacheProperties::No)
            .build()
            .await
            .map_err(Error::Bus)?;
        let managed = manager.get_managed_objects().await.map_err(fdo_error)?;
        let mut adapters = Vec::new();
        let mut devices = Vec::new();
        let interface = |name: &str| OwnedInterfaceName::try_from(name).expect("a valid name");
        let (adapter_interface, device_interface) = (interface(ADAPTER), interface(DEVICE));
        for (path, interfaces) in managed {
            if let Some(properties) = interfaces.get(&adapter_interface) {
                adapters.push(Adapter {
                    path: path.clone(),
                    powered: boolean(properties, "Powered"),
                });
            }
            let Some(properties) = interfaces.get(&device_interface) else {
                continue;
            };
            let address: Option<Address> =
                string(properties, "Address").and_then(|address| address.parse().ok());
            let adapter = properties
                .get("Adapter")
                .and_then(|value| OwnedObjectPath::try_from(value.try_clone().ok()?).ok());
            let (Some(address), Some(adapter)) = (address, adapter) else {
                continue;
            };
            devices.push(Device {
                path,
                adapter,
                paired: boolean(properties, "Paired"),
                seen: properties.contains_key("RSSI"),
                peripheral: Paired {
                    alias: string(properties, "Alias")
                        .or_else(|| string(properties, "Name"))
                        .unwrap_or_else(|| address.to_string()),
                    address,
                    connected: boolean(properties, "Connected"),
                },
            });
        }
        adapters.sort_by(|a, b| a.path.cmp(&b.path));
        devices.sort_by(|a, b| a.path.cmp(&b.path));
        if adapters.is_empty() {
            return Err(Error::NoAdapter);
        }
        Ok(Objects { adapters, devices })
    }

    pub async fn paired(&self) -> Result<Option<Vec<Paired>>, Error> {
        let objects = match self.objects().await {
            Ok(objects) => objects,
            Err(Error::NoAdapter) => return Ok(None),
            Err(error) => return Err(error),
        };
        // One per address, should it be paired on two adapters.
        let paired: BTreeMap<&Address, &Paired> = objects
            .devices
            .iter()
            .rev()
            .filter(|device| device.paired)
            .map(|device| (&device.peripheral.address, &device.peripheral))
            .collect();
        Ok(Some(paired.into_values().cloned().collect()))
    }

    async fn adapter_proxy(&self, path: &OwnedObjectPath) -> Result<Adapter1Proxy<'static>, Error> {
        Adapter1Proxy::builder(&self.connection)
            .path(path.clone())
            .cache_properties(CacheProperties::No)
            .build()
            .await
            .map_err(Error::Bus)
    }

    async fn device_proxy(&self, path: &OwnedObjectPath) -> Result<Device1Proxy<'static>, Error> {
        Device1Proxy::builder(&self.connection)
            .path(path.clone())
            .cache_properties(CacheProperties::No)
            .build()
            .await
            .map_err(Error::Bus)
    }

    /// Powers `adapter` on, unless it is.
    async fn power_on(&self, adapter: &Adapter) -> Result<(), Error> {
        if adapter.powered {
            return Ok(());
        }
        let proxy = self.adapter_proxy(&adapter.path).await?;
        proxy.set_powered(true).await.map_err(bluez_error)
    }

    /// The paired device at `address`, with its adapter powered on.
    async fn powered(&self, address: &Address) -> Result<OwnedObjectPath, Error> {
        let objects = self.objects().await?;
        let device = objects.paired(address).ok_or(Error::NotPaired)?;
        if let Some(adapter) = objects.adapter(&device.adapter) {
            self.power_on(adapter).await?;
        }
        Ok(device.path.clone())
    }

    pub async fn connect(&self, address: &Address) -> Result<(), Error> {
        let path = self.powered(address).await?;
        let device = self.device_proxy(&path).await?;
        let deadline = Instant::now() + CONNECT_RETRIES;
        loop {
            let error = match device.connect().await.map_err(bluez_error) {
                Ok(()) => return Ok(()),
                Err(error) if is_bluez(&error, "AlreadyConnected") => return Ok(()),
                Err(error) => error,
            };
            if Instant::now() + CONNECT_RETRY_DELAY > deadline {
                return Err(error);
            }
            tracing::debug!(%address, %error, "connecting failed; retrying");
            tokio::time::sleep(CONNECT_RETRY_DELAY).await;
            // It may have connected to this Node by itself meanwhile.
            let objects = self.objects().await?;
            if objects
                .paired(address)
                .is_some_and(|device| device.peripheral.connected)
            {
                return Ok(());
            }
        }
    }

    pub async fn disconnect(&self, address: &Address) -> Result<(), Error> {
        let objects = self.objects().await?;
        let device = objects.paired(address).ok_or(Error::NotPaired)?;
        if !device.peripheral.connected {
            return Ok(());
        }
        let proxy = self.device_proxy(&device.path).await?;
        match proxy.disconnect().await.map_err(bluez_error) {
            Err(error) if !is_bluez(&error, "NotConnected") => Err(error),
            _ => Ok(()),
        }
    }

    pub async fn forget(&self, address: &Address) -> Result<(), Error> {
        let objects = self.objects().await?;
        let paired: Vec<&Device> = objects
            .devices
            .iter()
            .filter(|device| device.paired && device.peripheral.address == *address)
            .collect();
        if paired.is_empty() {
            return Err(Error::NotPaired);
        }
        for device in paired {
            let adapter = self.adapter_proxy(&device.adapter).await?;
            adapter
                .remove_device(&device.path.as_ref())
                .await
                .map_err(bluez_error)?;
        }
        Ok(())
    }

    pub async fn set_alias(&self, address: &Address, alias: &str) -> Result<(), Error> {
        let objects = self.objects().await?;
        let device = objects.paired(address).ok_or(Error::NotPaired)?;
        let proxy = self.device_proxy(&device.path).await?;
        proxy.set_alias(alias).await.map_err(bluez_error)
    }

    pub async fn pair(&self, address: &Address) -> Result<(), Error> {
        let Ok(pairing) = self.pairing.try_lock() else {
            return Err(Error::Busy);
        };
        let objects = self.objects().await?;
        if objects.paired(address).is_some() {
            return Err(Error::AlreadyPaired);
        }
        let adapter = objects.adapters.first().ok_or(Error::NoAdapter)?;
        self.power_on(adapter).await?;
        let adapter = self.adapter_proxy(&adapter.path).await?;
        adapter.start_discovery().await.map_err(bluez_error)?;
        let found = self.scan(&adapter, address).await;
        // Pairing goes better without a scan alongside it.
        if let Err(error) = adapter.stop_discovery().await {
            tracing::debug!(%error, "can't stop scanning");
        }
        let path = found?;
        let asked_for_code = Arc::new(AtomicBool::new(false));
        let agent = Agent {
            device: path.clone(),
            asked_for_code: asked_for_code.clone(),
        };
        let paired = self.pair_with_agent(&path, agent).await;
        if paired.is_err() && asked_for_code.load(Ordering::Relaxed) {
            return Err(Error::NeedsCode);
        }
        paired?;
        let device = self.device_proxy(&path).await?;
        // So that it can reconnect by itself and use its profiles, such as
        // audio, without asking.
        device.set_trusted(true).await.map_err(bluez_error)?;
        drop(pairing);
        Ok(())
    }

    /// Waits until the scan on `adapter` sees `address`, and returns its
    /// device.
    async fn scan(
        &self,
        adapter: &Adapter1Proxy<'_>,
        address: &Address,
    ) -> Result<OwnedObjectPath, Error> {
        let deadline = Instant::now() + SCAN;
        loop {
            let objects = self.objects().await?;
            let seen = objects.devices.iter().find(|device| {
                device.peripheral.address == *address
                    && device.adapter.as_str() == adapter.inner().path().as_str()
                    && device.seen
            });
            if let Some(device) = seen {
                return Ok(device.path.clone());
            }
            if Instant::now() >= deadline {
                return Err(Error::NotSeen);
            }
            tokio::time::sleep(SCAN_POLL).await;
        }
    }

    /// Pairs the device at `path`, with `agent` answering BlueZ.
    async fn pair_with_agent(&self, path: &OwnedObjectPath, agent: Agent) -> Result<(), Error> {
        let agent_path = ObjectPath::from_static_str_unchecked(AGENT_PATH);
        let server = self.connection.object_server();
        server.at(&agent_path, agent).await.map_err(Error::Bus)?;
        let manager = AgentManager1Proxy::builder(&self.connection)
            .cache_properties(CacheProperties::No)
            .build()
            .await
            .map_err(Error::Bus)?;
        let paired = async {
            manager
                .register_agent(&agent_path, "NoInputNoOutput")
                .await
                .map_err(bluez_error)?;
            let device = self.device_proxy(path).await?;
            let paired = device.pair().await.map_err(bluez_error);
            if let Err(error) = manager.unregister_agent(&agent_path).await {
                tracing::debug!(%error, "can't unregister the pairing agent");
            }
            paired
        }
        .await;
        if let Err(error) = server.remove::<Agent, _>(&agent_path).await {
            tracing::debug!(%error, "can't stop serving the pairing agent");
        }
        paired
    }
}

/// Whether `signal` says something the UI shows changed: a property of
/// [`SHOWN`] on an adapter or device, or a paired device appearing.
fn shown(signal: &zbus::Message) -> bool {
    let header = signal.header();
    let body = signal.body();
    match header.member().map(|member| member.as_str()) {
        Some("PropertiesChanged") => body
            .deserialize::<(&str, HashMap<&str, zbus::Variant<'_>>, Vec<&str>)>()
            .is_ok_and(|(interface, changed, invalidated)| {
                [ADAPTER, DEVICE].contains(&interface)
                    && changed
                        .keys()
                        .chain(&invalidated)
                        .any(|name| SHOWN.contains(name))
            }),
        Some("InterfacesAdded") => body
            .deserialize::<(ObjectPath<'_>, HashMap<&str, Properties>)>()
            .is_ok_and(|(_, interfaces)| {
                interfaces
                    .get(DEVICE)
                    .is_some_and(|properties| boolean(properties, "Paired"))
            }),
        Some("InterfacesRemoved") => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties_changed(interface: &str, changed: &[(&str, bool)]) -> zbus::Message {
        let changed: HashMap<&str, zbus::Variant<'_>> = changed
            .iter()
            .map(|(name, value)| (*name, zbus::Variant::from(*value)))
            .collect();
        zbus::Message::signal(
            "/org/bluez/hci0/dev_98_47_44_A6_21_19",
            "org.freedesktop.DBus.Properties",
            "PropertiesChanged",
        )
        .build(&(interface, changed, Vec::<&str>::new()))
        .unwrap()
    }

    #[test]
    fn only_changes_the_ui_shows_count() {
        assert!(shown(&properties_changed(DEVICE, &[("Connected", true)])));
        assert!(shown(&properties_changed(ADAPTER, &[("Powered", false)])));
        // A scan's churn.
        assert!(!shown(&properties_changed(
            DEVICE,
            &[("ServicesResolved", true)]
        )));
        assert!(!shown(&properties_changed(
            ADAPTER,
            &[("Discovering", true)]
        )));
        assert!(!shown(&properties_changed(
            "org.bluez.MediaTransport1",
            &[("Connected", true)]
        )));
    }
}
