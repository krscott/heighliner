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
    /// Ask another Node's daemon, by machine name or node ID, rather than
    /// this one's. Paths without a Node are then that Node's, and must be
    /// absolute or start with ~.
    #[arg(long, global = true, value_name = "NODE")]
    on: Option<String>,
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
        /// Days to keep superseded configuration, and the displaced versions
        /// of items without their own retention.
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=36_500))]
        retention_days: u64,
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
    /// Show the items and their replicas, with this Node's replicas'
    /// states and the other Nodes' as last read.
    Status,
    /// List this Node's data conflicts, or keep or swap one.
    Conflicts {
        #[command(subcommand)]
        action: Option<ConflictAction>,
    },
    /// Open this Node's web UI, or another's with --on, in a browser.
    Ui,
    /// Leave the Fleet: no Node syncs with this one any more. Its replicas
    /// stay until they're removed, and nothing is deleted.
    Leave {
        /// Confirm it; a Node that leaves isn't expected back.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum ConflictAction {
    /// Keep the change that won; the other is kept until it expires.
    Keep {
        /// The conflict's item, by name or ID.
        item: String,
        /// The conflict's version, as listed.
        version: String,
    },
    /// Make the change that lost the file's content again; the other is
    /// kept until it expires.
    Swap { item: String, version: String },
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
    let (port, tailscale_socket, on) = (cli.port, cli.tailscale_socket, cli.on);
    let daemon = matches!(cli.command, Command::Daemon { .. });
    let result = match cli.command {
        Command::Daemon {
            state_dir,
            pull_every,
            retention_days,
            syncthing_socket,
            syncthing_api_key_file,
        } => run_daemon(daemon::Options {
            state_dir,
            port,
            tailscale_socket,
            pull_every: Duration::from_secs(pull_every),
            retention: Duration::from_secs(retention_days * 24 * 60 * 60),
            syncthing: syncthing_socket.zip(syncthing_api_key_file).map(
                |(socket, api_key_file)| daemon::SyncthingOptions {
                    socket,
                    api_key_file,
                },
            ),
        })
        .map_err(|error| error.to_string()),
        command => runtime().block_on(run_command(command, port, tailscale_socket, on)),
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
async fn run_command(
    command: Command,
    port: u16,
    tailscale_socket: PathBuf,
    on: Option<String>,
) -> Result<(), String> {
    let on = on.as_deref();
    let tailscale = tailscale::Client::new(tailscale_socket);
    let client = cli::Client::connect(&tailscale, port, on)
        .await
        .map_err(|error| error.to_string())?;
    // A relative path is this machine's only when asking its own daemon.
    let cwd = if client.is_here() {
        Some(
            std::env::current_dir()
                .map_err(|error| format!("can't tell the current directory: {error}"))?,
        )
    } else {
        None
    };
    let me = client.me().as_str().to_owned();
    let local = [me.as_str(), client.name()];
    let done = match command {
        Command::Daemon { .. } => unreachable!("run apart"),
        Command::Track {
            path,
            name,
            one_way,
            excludes,
            retention_days,
        } => {
            let (node, path) = cli::node_path(&path, cwd.as_deref(), &local);
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
            let (node, path) = cli::node_path(&path, cwd.as_deref(), &local);
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
        Command::Conflicts { action: None } => client.conflicts().await.map(|listed| {
            print!("{}", cli::format_conflicts(&listed));
        }),
        Command::Conflicts {
            action: Some(ConflictAction::Keep { item, version }),
        } => client.keep(&api::Resolve { item, version }).await,
        Command::Conflicts {
            action: Some(ConflictAction::Swap { item, version }),
        } => client.swap(&api::Resolve { item, version }).await,
        Command::Ui => match client.status().await {
            Ok(_) => {
                let url = client.url();
                if !open_browser(&url) {
                    println!("Open {url} in a browser.");
                }
                Ok(())
            }
            Err(error) => Err(error),
        },
        Command::Leave { yes: false } => {
            return Err("leaving can't be undone: no Node will sync with this one any more; run it again with --yes to leave".into());
        }
        Command::Leave { yes: true } => client.leave().await.map(|()| {
            println!("{} has left the Fleet.", client.name());
        }),
    };
    done.map_err(|error| error.to_string())
}

/// Opens `url` in the desktop's browser, and says whether that worked.
fn open_browser(url: &str) -> bool {
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    std::process::Command::new(opener)
        .arg(url)
        .status()
        .is_ok_and(|status| status.success())
}

fn run_daemon(options: daemon::Options) -> Result<(), daemon::Error> {
    let runtime = runtime();
    let result = runtime.block_on(daemon::run(options));
    // The daemon has saved its state by now. A path check stuck on a hung
    // filesystem would otherwise keep the process from exiting.
    runtime.shutdown_background();
    result
}
