use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use heighliner::daemon::{self, DEFAULT_PORT};
use heighliner::tailscale::DEFAULT_SOCKET;

/// Tracks directories across a Tailscale fleet and keeps them replicated
/// with Syncthing.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run this Node's daemon.
    Daemon {
        /// Where the daemon keeps its state.
        #[arg(long, default_value = "/var/lib/heighliner")]
        state_dir: PathBuf,
        /// The port to serve on, on each of this device's Tailscale
        /// addresses. Every Node must use the same port.
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
        /// tailscaled's local API socket.
        #[arg(long, default_value = DEFAULT_SOCKET)]
        tailscale_socket: PathBuf,
        /// Seconds between pulls of the other Nodes' records.
        #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
        pull_every: u64,
        /// Syncthing's GUI and REST API socket. Without it, the daemon
        /// doesn't drive Syncthing.
        #[arg(long, requires = "syncthing_api_key_file")]
        syncthing_socket: Option<PathBuf>,
        /// A file holding Syncthing's API key.
        #[arg(long, requires = "syncthing_socket")]
        syncthing_api_key_file: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let terminal = std::io::stderr().is_terminal();
    let logs = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(terminal)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        );
    // Elsewhere, such as under systemd, the log adds its own timestamps.
    if terminal {
        logs.init();
    } else {
        logs.without_time().init();
    }
    let result = match Cli::parse().command {
        Command::Daemon {
            state_dir,
            port,
            tailscale_socket,
            pull_every,
            syncthing_socket,
            syncthing_api_key_file,
        } => run_daemon(daemon::Options {
            state_dir,
            port,
            tailscale_socket,
            pull_every: Duration::from_secs(pull_every),
            syncthing: syncthing_socket.zip(syncthing_api_key_file).map(
                |(socket, api_key_file)| daemon::SyncthingOptions {
                    socket,
                    api_key_file,
                },
            ),
        }),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run_daemon(options: daemon::Options) -> Result<(), daemon::Error> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime starts")
        .block_on(daemon::run(options))
}
