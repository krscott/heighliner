//! The Fleet's Peripherals: what each Node has paired, asked of each
//! reachable Node as it is needed, and the actions that move them between
//! Nodes by commanding each Node involved (ADR-0006). Nothing about them
//! is stored.
//!
//! Actions run in tasks of their own, so that a client that gives up, such
//! as a phone whose page closes, can't cut one short halfway, as between
//! disconnecting a Peripheral and connecting it elsewhere, or between
//! starting a scan and stopping it.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use tokio::task::JoinSet;

use super::api::{Answer, Api, Refusal, invalid, rejected};
use super::signal;
use crate::api::{
    self, BluetoothNode, BluetoothState, Done, PairedOn, Peripheral, PeripheralAction, Peripherals,
    Reach, Status,
};
use crate::bluetooth::{self, Address, Paired};
use crate::model::NodeId;
use crate::protocol::{self, Command, NodePeripherals};
use crate::tailscale::LocalApi;

/// How long a Node, this one included, may take to list its Peripherals.
const LIST_TIMEOUT: Duration = Duration::from_secs(5);

/// How long another Node may take to carry out a command, which may
/// retry connecting for a while.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// How long another Node may take to pair, which scans first.
const PAIR_TIMEOUT: Duration = Duration::from_secs(90);

pub(super) fn router<T: LocalApi + Clone>(api: Api<T>) -> Router {
    Router::new()
        .route(api::PERIPHERALS, get(list::<T>))
        .route("/api/v1/peripherals/{peripheral}", post(act::<T>))
        .route(protocol::PERIPHERALS_PATH, get(node_list::<T>))
        .route("/node/v1/peripherals/{address}", post(node_command::<T>))
        .with_state(api)
}

/// One Node, as asked for its Peripherals.
struct Asked {
    node: BluetoothNode,
    /// Where it was asked, for another Node.
    addr: Option<SocketAddr>,
    paired: Vec<Paired>,
}

/// What every Node of the Fleet said, this one first.
struct Survey {
    me: NodeId,
    nodes: Vec<Asked>,
}

impl Survey {
    fn view(&self) -> Peripherals {
        let mut peripherals: Vec<Peripheral> = Vec::new();
        for asked in &self.nodes {
            for paired in &asked.paired {
                let on = PairedOn {
                    node: asked.node.node.clone(),
                    alias: paired.alias.clone(),
                    connected: paired.connected,
                };
                match peripherals
                    .iter_mut()
                    .find(|known| known.address == paired.address)
                {
                    Some(known) => known.paired.push(on),
                    None => peripherals.push(Peripheral {
                        address: paired.address.clone(),
                        alias: paired.alias.clone(),
                        paired: vec![on],
                    }),
                }
            }
        }
        peripherals.sort_by(|a, b| (&a.alias, &a.address).cmp(&(&b.alias, &b.address)));
        Peripherals {
            nodes: self.nodes.iter().map(|asked| asked.node.clone()).collect(),
            peripherals,
        }
    }

    fn asked(&self, node: &NodeId) -> Option<&Asked> {
        self.nodes.iter().find(|asked| asked.node.node == *node)
    }

    /// The Nodes that may hold a Peripheral without saying so, by name.
    fn unknown(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|asked| {
                !matches!(
                    asked.node.bluetooth,
                    BluetoothState::Ready | BluetoothState::NoAdapter
                )
            })
            .map(|asked| name(&asked.node))
            .collect()
    }
}

fn name(node: &BluetoothNode) -> &str {
    node.name.as_deref().unwrap_or(node.node.as_str())
}

/// What another Node's answer says of its Bluetooth.
fn answered(answer: Result<NodePeripherals, protocol::Error>) -> (BluetoothState, Vec<Paired>) {
    match answer {
        Ok(NodePeripherals { peripherals: None }) => (BluetoothState::NoAdapter, Vec::new()),
        Ok(NodePeripherals {
            peripherals: Some(peripherals),
        }) => (BluetoothState::Ready, peripherals),
        Err(error) if error.is_not_found() => (BluetoothState::Older, Vec::new()),
        Err(error) if error.is_unreachable() => (BluetoothState::Unreachable, Vec::new()),
        Err(error) => (
            BluetoothState::Failed {
                error: error.to_string(),
            },
            Vec::new(),
        ),
    }
}

/// The status a failed local Bluetooth operation is answered with.
fn refusal(error: &bluetooth::Error) -> StatusCode {
    use bluetooth::Error;
    match error {
        Error::NotPaired => StatusCode::NOT_FOUND,
        Error::NoAdapter | Error::AlreadyPaired | Error::NeedsCode | Error::Busy => {
            StatusCode::CONFLICT
        }
        Error::NotSeen => StatusCode::GATEWAY_TIMEOUT,
        Error::Bluez { .. } => StatusCode::BAD_GATEWAY,
        Error::Bus(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// The Peripheral of `view` that `name` names: by address, or by a name
/// one of its Nodes gives it, if only one has it.
fn find<'a>(view: &'a Peripherals, name: &str) -> Result<&'a Peripheral, Refusal> {
    if let Ok(address) = name.parse::<Address>() {
        return view
            .peripherals
            .iter()
            .find(|peripheral| peripheral.address == address)
            .ok_or_else(|| {
                Refusal(
                    StatusCode::NOT_FOUND,
                    format!("no Node that could be asked has {address} paired"),
                )
            });
    }
    let matching: Vec<&Peripheral> = view
        .peripherals
        .iter()
        .filter(|peripheral| peripheral.paired.iter().any(|on| on.alias == name))
        .collect();
    match &matching[..] {
        [peripheral] => Ok(peripheral),
        [] => Err(Refusal(
            StatusCode::NOT_FOUND,
            format!("no Node that could be asked has a Peripheral named {name}"),
        )),
        _ => {
            let addresses: Vec<&str> = matching
                .iter()
                .map(|peripheral| peripheral.address.as_str())
                .collect();
            Err(Refusal(
                StatusCode::CONFLICT,
                format!(
                    "{} Peripherals are named {name}; use one of their addresses: {}",
                    matching.len(),
                    addresses.join(", ")
                ),
            ))
        }
    }
}

/// `message`, then the `notes` gathered on the way, as one sentence.
fn with_notes(message: String, notes: &[String]) -> String {
    std::iter::once(message)
        .chain(notes.iter().cloned())
        .collect::<Vec<_>>()
        .join("; ")
}

impl<T: LocalApi + Clone> Api<T> {
    /// This Node's own Peripherals, or `None` if it has no Bluetooth.
    async fn paired_here(&self) -> Result<Option<Vec<Paired>>, String> {
        let Some(bluetooth) = &self.bluetooth else {
            return Ok(None);
        };
        match tokio::time::timeout(LIST_TIMEOUT, bluetooth.paired()).await {
            Ok(paired) => paired.map_err(|error| error.to_string()),
            Err(_) => Err("BlueZ didn't answer in time".to_owned()),
        }
    }

    /// Asks every Node of `status` that its latest round reached for its
    /// Peripherals, at once.
    async fn survey(&self, status: Status) -> Survey {
        let mut asking = JoinSet::new();
        let mut nodes: Vec<Asked> = Vec::new();
        for (index, node) in status
            .nodes
            .into_iter()
            .filter(|node| !node.departed)
            .enumerate()
        {
            let mut addr = None;
            let (bluetooth, paired) = match (&node.reach, node.ip) {
                (Reach::Here, _) => match self.paired_here().await {
                    Ok(Some(paired)) => (BluetoothState::Ready, paired),
                    Ok(None) => (BluetoothState::NoAdapter, Vec::new()),
                    Err(error) => (BluetoothState::Failed { error }, Vec::new()),
                },
                (Reach::Live { .. }, Some(ip)) => {
                    let at = SocketAddr::new(ip, self.port);
                    addr = Some(at);
                    asking.spawn(
                        async move { (index, protocol::peripherals(at, LIST_TIMEOUT).await) },
                    );
                    // Until it answers.
                    (BluetoothState::Unreachable, Vec::new())
                }
                (Reach::Incompatible { .. }, _) => (BluetoothState::Incompatible, Vec::new()),
                _ => (BluetoothState::Unreachable, Vec::new()),
            };
            let node = BluetoothNode {
                node: node.node,
                name: node.name,
                bluetooth,
            };
            nodes.push(Asked { node, addr, paired });
        }
        while let Some(joined) = asking.join_next().await {
            let (index, answer) = joined.expect("asking tasks don't panic");
            let asked = &mut nodes[index];
            (asked.node.bluetooth, asked.paired) = answered(answer);
        }
        Survey {
            me: status.node,
            nodes,
        }
    }

    /// The Fleet's Peripherals, as each Node of `status` that could be
    /// asked has them.
    pub(super) async fn peripherals(&self, status: Status) -> Peripherals {
        self.survey(status).await.view()
    }

    /// Carries out `command` on this Node's Peripheral at `address`.
    async fn command_here(
        &self,
        address: Address,
        command: Command,
    ) -> Result<(), bluetooth::Error> {
        let bluetooth = self.bluetooth.clone().ok_or(bluetooth::Error::NoAdapter)?;
        let done = tokio::spawn(async move {
            match command {
                Command::Connect => bluetooth.connect(&address).await,
                Command::Disconnect => bluetooth.disconnect(&address).await,
                Command::Pair => bluetooth.pair(&address).await,
                Command::Forget => bluetooth.forget(&address).await,
                Command::Name { alias } => bluetooth.set_alias(&address, &alias).await,
            }
        })
        .await
        .expect("Bluetooth commands don't panic");
        signal(&self.changes);
        done
    }

    /// Has `node` carry out `command` on its Peripheral at `address`.
    async fn command(
        &self,
        survey: &Survey,
        node: &NodeId,
        address: &Address,
        command: Command,
    ) -> Result<(), String> {
        if *node == survey.me {
            return self
                .command_here(address.clone(), command)
                .await
                .map_err(|error| error.to_string());
        }
        let addr = survey
            .asked(node)
            .and_then(|asked| asked.addr)
            .ok_or_else(|| "it can't be reached".to_owned())?;
        let timeout = match command {
            Command::Pair => PAIR_TIMEOUT,
            _ => COMMAND_TIMEOUT,
        };
        protocol::command(addr, address, &command, timeout)
            .await
            .map_err(|error| error.to_string())
    }

    /// The Node `name` names, which must have listed its Peripherals.
    async fn ready<'a>(
        &self,
        survey: &'a Survey,
        name: Option<&str>,
    ) -> Result<&'a BluetoothNode, Refusal> {
        let node = self.node(name, false).await?;
        let asked = survey.asked(&node).ok_or_else(|| {
            Refusal(
                StatusCode::NOT_FOUND,
                format!("{} isn't a Node of the Fleet", name.unwrap_or("this")),
            )
        })?;
        match asked.node.bluetooth.problem() {
            None => Ok(&asked.node),
            Some(problem) => Err(Refusal(
                StatusCode::CONFLICT,
                format!("{} {problem}", self::name(&asked.node)),
            )),
        }
    }

    /// Disconnects `peripheral` from every Node it is connected to but
    /// `target`, all at once, and says where that failed.
    async fn disconnect_elsewhere(
        &self,
        survey: &Survey,
        view: &Peripherals,
        peripheral: &Peripheral,
        target: &NodeId,
    ) -> Vec<String> {
        let disconnecting = peripheral
            .paired
            .iter()
            .filter(|on| on.connected && on.node != *target)
            .map(|on| async move {
                let disconnected = self
                    .command(survey, &on.node, &peripheral.address, Command::Disconnect)
                    .await;
                disconnected.err().map(|error| {
                    let node = view.node_name(&on.node);
                    format!("couldn't disconnect it from {node}: {error}")
                })
            });
        futures_util::future::join_all(disconnecting)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Where `peripheral` may still be connected, after connecting it
    /// elsewhere failed.
    fn holders(survey: &Survey) -> Option<String> {
        let unknown = survey.unknown();
        (!unknown.is_empty()).then(|| {
            format!(
                "it may be connected to a Node that couldn't be asked: {}",
                unknown.join(", ")
            )
        })
    }

    /// Carries out `action` on the Peripheral `name` names, to the end even
    /// if the request is dropped.
    pub(super) async fn act_on_peripheral(
        &self,
        name: &str,
        action: PeripheralAction,
    ) -> Result<Done, Refusal> {
        let (api, name) = (self.clone(), name.to_owned());
        tokio::spawn(async move { api.act(&name, action).await })
            .await
            .expect("Peripheral actions don't panic")
    }

    async fn act(&self, name: &str, action: PeripheralAction) -> Result<Done, Refusal> {
        let survey = self.survey(self.status().await).await;
        let view = survey.view();
        let peripheral = find(&view, name)?;
        let alias = &peripheral.alias;
        let address = &peripheral.address;
        let paired_on = |node: &NodeId| peripheral.paired.iter().find(|on| on.node == *node);
        let failed = |message: String| Refusal(StatusCode::BAD_GATEWAY, message);
        match action {
            PeripheralAction::Handoff { node } => {
                let target = self.ready(&survey, node.as_deref()).await?;
                let target_name = self::name(target);
                if paired_on(&target.node).is_none() {
                    return Err(Refusal(
                        StatusCode::CONFLICT,
                        format!("{alias} isn't paired with {target_name}; pair it there first"),
                    ));
                }
                let mut notes = self
                    .disconnect_elsewhere(&survey, &view, peripheral, &target.node)
                    .await;
                let connected = self
                    .command(&survey, &target.node, address, Command::Connect)
                    .await;
                if let Err(error) = connected {
                    notes.extend(Self::holders(&survey));
                    let message = format!("couldn't connect {alias} on {target_name}: {error}");
                    return Err(failed(with_notes(message, &notes)));
                }
                Ok(Done { notes })
            }
            PeripheralAction::Pair { node } => {
                let target = self.ready(&survey, node.as_deref()).await?;
                let target_name = self::name(target);
                if paired_on(&target.node).is_some() {
                    return Err(Refusal(
                        StatusCode::CONFLICT,
                        format!("{alias} is already paired with {target_name}"),
                    ));
                }
                let mut notes = self
                    .disconnect_elsewhere(&survey, &view, peripheral, &target.node)
                    .await;
                let paired = self
                    .command(&survey, &target.node, address, Command::Pair)
                    .await;
                if let Err(error) = paired {
                    let message = format!("couldn't pair {alias} with {target_name}: {error}");
                    return Err(failed(with_notes(message, &notes)));
                }
                // Its other Nodes' name, if they agree on one.
                if let [(alias, _)] = &peripheral.aliases()[..] {
                    let alias = (*alias).to_owned();
                    let command = Command::Name {
                        alias: alias.clone(),
                    };
                    let named = self.command(&survey, &target.node, address, command).await;
                    if let Err(error) = named {
                        notes.push(format!(
                            "couldn't name it {alias} on {target_name}: {error}"
                        ));
                    }
                }
                let connected = self
                    .command(&survey, &target.node, address, Command::Connect)
                    .await;
                if let Err(error) = connected {
                    notes.push(format!(
                        "paired it, but couldn't connect it on {target_name}: {error}"
                    ));
                }
                Ok(Done { notes })
            }
            PeripheralAction::Disconnect { node } => {
                self.on_one_node(
                    &survey,
                    peripheral,
                    node.as_deref(),
                    Command::Disconnect,
                    "disconnect",
                )
                .await
            }
            PeripheralAction::Forget { node } => {
                self.on_one_node(
                    &survey,
                    peripheral,
                    node.as_deref(),
                    Command::Forget,
                    "forget",
                )
                .await
            }
            PeripheralAction::Name { alias: chosen } => {
                let aliases = peripheral.aliases();
                if !aliases.iter().any(|(alias, _)| *alias == chosen) {
                    let known: Vec<&str> = aliases.iter().map(|(alias, _)| *alias).collect();
                    return Err(invalid(format!(
                        "none of its Nodes names it {chosen}; pick one of: {}",
                        known.join(", ")
                    )));
                }
                let mut named = Vec::new();
                let mut failures = Vec::new();
                for on in peripheral.paired.iter().filter(|on| on.alias != chosen) {
                    let node = view.node_name(&on.node);
                    let command = Command::Name {
                        alias: chosen.clone(),
                    };
                    match self.command(&survey, &on.node, address, command).await {
                        Ok(()) => named.push(node),
                        Err(error) => failures.push(format!("couldn't name it on {node}: {error}")),
                    }
                }
                if !failures.is_empty() {
                    let message = match &named[..] {
                        [] => format!("didn't name it {chosen} anywhere"),
                        named => format!("named it {chosen} on {}", named.join(", ")),
                    };
                    return Err(failed(with_notes(message, &failures)));
                }
                Ok(Done::default())
            }
        }
    }

    /// Has the Node `node` names carry out `command` on `peripheral`, which
    /// must be paired there; `what` says what it does.
    async fn on_one_node(
        &self,
        survey: &Survey,
        peripheral: &Peripheral,
        node: Option<&str>,
        command: Command,
        what: &str,
    ) -> Result<Done, Refusal> {
        let target = self.ready(survey, node).await?;
        let (alias, target_name) = (&peripheral.alias, name(target));
        if !peripheral.paired.iter().any(|on| on.node == target.node) {
            return Err(Refusal(
                StatusCode::CONFLICT,
                format!("{alias} isn't paired with {target_name}"),
            ));
        }
        self.command(survey, &target.node, &peripheral.address, command)
            .await
            .map_err(|error| {
                Refusal(
                    StatusCode::BAD_GATEWAY,
                    format!("couldn't {what} {alias} on {target_name}: {error}"),
                )
            })?;
        Ok(Done::default())
    }
}

async fn list<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Json<Peripherals> {
    Json(api.peripherals(api.status().await).await)
}

async fn act<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(peripheral): Path<String>,
    action: Result<Json<PeripheralAction>, JsonRejection>,
) -> Answer<Done> {
    let Json(action) = action.map_err(rejected)?;
    Ok(Json(api.act_on_peripheral(&peripheral, action).await?))
}

/// This Node's Peripherals, for the other Nodes (Node protocol).
async fn node_list<T: LocalApi + Clone>(State(api): State<Api<T>>) -> Answer<NodePeripherals> {
    let peripherals = api
        .paired_here()
        .await
        .map_err(|error| Refusal(StatusCode::SERVICE_UNAVAILABLE, error))?;
    Ok(Json(NodePeripherals { peripherals }))
}

/// Carries out another Node's command on one of this Node's Peripherals
/// (Node protocol).
async fn node_command<T: LocalApi + Clone>(
    State(api): State<Api<T>>,
    Path(address): Path<String>,
    command: Result<Json<Command>, JsonRejection>,
) -> Answer<()> {
    let Json(command) = command.map_err(rejected)?;
    let address: Address = address.parse().map_err(invalid)?;
    api.command_here(address, command)
        .await
        .map_err(|error| Refusal(refusal(&error), error.to_string()))?;
    Ok(Json(()))
}

#[cfg(test)]
mod tests;
