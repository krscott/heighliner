//! Heighliner is a control plane for a Tailscale fleet: it keeps
//! directories replicated with Syncthing and moves Bluetooth Peripherals
//! between Nodes.

pub mod api;
pub mod auth;
pub mod bluetooth;
pub mod cli;
pub mod config;
pub mod daemon;
pub mod discovery;
pub mod hlc;
mod http;
pub mod model;
pub mod protocol;
pub mod record;
pub mod syncthing;
pub mod tailscale;
