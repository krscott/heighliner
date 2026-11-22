//! Heighliner tracks directories across a Tailscale fleet and keeps them
//! replicated with Syncthing.

pub mod api;
pub mod auth;
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
