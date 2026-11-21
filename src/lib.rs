//! Heighliner tracks directories across a Tailscale fleet and keeps them
//! replicated with Syncthing.

pub mod auth;
pub mod config;
pub mod hlc;
pub mod model;
pub mod record;
pub mod tailscale;
