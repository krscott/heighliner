use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use std::num::NonZeroU32;

use heighliner::daemon::{self, DEFAULT_PORT};
use heighliner::tailscale::{self, DEFAULT_SOCKET};
use heighliner::{api, cli};

/// Tracks directories across a Tailscale fleet and keeps them replicated
/// with Syncthing.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// The port heighliner serves on, on each of this device's Tailscale
    /// addresses. Every Node must use the same port.
    #[arg(long, global = true, default_value_t = DEFAULT_PORT)]
    port: u16,
    /// tailscaled's local API socket.
    #[arg(long, global = true, default_value = DEFAULT_SOCKET)]
    tailscale_socket: PathBuf,
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
    /// Track a directory as a new item, adopting the files already in it.
    Track {
        /// `[node:]path`; a path on another Node must be absolute.
        path: String,
        /// The item's name; the directory's name if none.
        #[arg(long)]
        name: Option<String>,
        /// Make it one-way, from this replica.
        #[arg(long)]
        one_way: bool,
        /// A Syncthing ignore pattern for files not to sync; repeatable.
        #[arg(long = "exclude", value_name = "PATTERN")]
        excludes: Vec<String>,
        /// Keep displaced versions this many days instead of 30.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
        retention_days: Option<u32>,
    },
    /// Add a replica of an item.
    Sync {
        /// The item's name or ID.
        item: String,
        /// `[node:]path`; a path on another Node must be absolute.
        path: String,
        /// Only receive changes.
        #[arg(long)]
        mirror: bool,
        /// Start even if the path already holds files, merging them.
        #[arg(long)]
        adopt: bool,
    },
    /// Remove a replica of an item. Its files stay.
    RemoveReplica {
        item: String,
        /// Its Node; this one if none.
        node: Option<String>,
    },
    /// Stop tracking an item. Its files stay.
    Untrack { item: String },
    /// Pause an item, or one of its replicas.
    Pause {
        item: String,
        /// A replica's Node, to pause only it.
        node: Option<String>,
    },
    /// Resume an item, or one of its replicas.
    Resume {
        item: String,
        /// A replica's Node, to resume only it.
        node: Option<String>,
    },
    /// Show the items, their replicas, and this Node's replicas' states.
    Status,
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
    let cli = Cli::parse();
    let (port, tailscale_socket) = (cli.port, cli.tailscale_socket);
    let daemon = matches!(cli.command, Command::Daemon { .. });
    let result = match cli.command {
        Command::Daemon {
            state_dir,
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
        })
        .map_err(|error| error.to_string()),
        command => runtime().block_on(run_command(command, port, tailscale_socket)),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if daemon => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("heighliner: {error}");
            ExitCode::FAILURE
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime starts")
}

/// Runs a command against this Node's daemon.
async fn run_command(command: Command, port: u16, tailscale_socket: PathBuf) -> Result<(), String> {
    let tailscale = tailscale::Client::new(tailscale_socket);
    let client = cli::Client::connect(&tailscale, port)
        .await
        .map_err(|error| error.to_string())?;
    let cwd = std::env::current_dir()
        .map_err(|error| format!("can't tell the current directory: {error}"))?;
    let me = client.me().as_str().to_owned();
    let done = match command {
        Command::Daemon { .. } => unreachable!("run apart"),
        Command::Track {
            path,
            name,
            one_way,
            excludes,
            retention_days,
        } => {
            let (node, path) = cli::node_path(&path, &cwd);
            let track = api::Track {
                node,
                path,
                name,
                one_way,
                excludes,
                retention_days: retention_days.and_then(NonZeroU32::new),
            };
            client.track(&track).await.map(|tracked| {
                println!("Tracking it as item {}.", tracked.id);
            })
        }
        Command::Sync {
            item,
            path,
            mirror,
            adopt,
        } => {
            let (node, path) = cli::node_path(&path, &cwd);
            let add = api::AddReplica {
                node,
                path,
                mirror,
                adopt,
            };
            client.add_replica(&item, &add).await
        }
        Command::RemoveReplica { item, node } => {
            client
                .remove_replica(&item, node.as_deref().unwrap_or(&me))
                .await
        }
        Command::Untrack { item } => client.untrack(&item).await,
        Command::Pause { item, node: None } => client.pause_item(&item, true).await,
        Command::Pause {
            item,
            node: Some(node),
        } => client.pause_replica(&item, &node, true).await,
        Command::Resume { item, node: None } => client.pause_item(&item, false).await,
        Command::Resume {
            item,
            node: Some(node),
        } => client.pause_replica(&item, &node, false).await,
        Command::Status => client.status().await.map(|status| {
            print!("{}", cli::format_status(&status));
        }),
    };
    done.map_err(|error| error.to_string())
}

fn run_daemon(options: daemon::Options) -> Result<(), daemon::Error> {
    let runtime = runtime();
    let result = runtime.block_on(daemon::run(options));
    // The daemon has saved its state by now. A path check stuck on a hung
    // filesystem would otherwise keep the process from exiting.
    runtime.shutdown_background();
    result
}
