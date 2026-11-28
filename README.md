# heighliner

A control plane for your Tailscale fleet. It tracks directories across the fleet
and keeps them replicated with Syncthing, moves Bluetooth Peripherals, such
as a headset, from one Node to another, and turns your phone into a Node's
touchpad and keyboard. See [GLOSSARY.md](GLOSSARY.md) for the vocabulary
and [docs/adr](docs/adr) for the foundational decisions.

Each Node runs a heighliner daemon and a Syncthing instance that heighliner
configures completely. Nodes find each other over Tailscale, share the Fleet's
configuration, and keep accepting edits while they can't reach each other.
Syncthing only talks over the tailnet.

## Install

Heighliner ships as a NixOS module. Add it to each Node's configuration:

```nix
{
  inputs.heighliner.url = "github:krscott/heighliner";

  outputs = { nixpkgs, heighliner, ... }: {
    nixosConfigurations.laptop = nixpkgs.lib.nixosSystem {
      modules = [
        ./configuration.nix
        heighliner.nixosModules.default
        {
          services.tailscale.enable = true;
          services.heighliner = {
            enable = true;
            user = "alice";
          };
        }
      ];
    };
  };
}
```

Heighliner and Syncthing run as `user`, and only that user's Tailscale devices
can use them. The module opens heighliner's port (7130) and Syncthing's (22000)
on `tailscale0` only. See [nix/module.nix](nix/module.nix) for the other
options, such as `retentionDays`.

## Tailscale setup

- Log each Node in to Tailscale as the user who owns the Fleet. A tagged device
  isn't owned by a user, so it can't be a Node.
- With the default policy, which allows all traffic, nothing else is needed.
  With your own policy, let your devices reach each other on heighliner's and
  Syncthing's ports:

  ```json
  "grants": [
    { "src": ["autogroup:member"], "dst": ["autogroup:self"], "ip": ["tcp:7130", "tcp:22000"] }
  ]
  ```

- Enable network map caching, so a Node that restarts while Tailscale's control
  server is out of reach starts from its last network map, and can usually
  reach the others through a DERP relay. It needs Tailscale 1.104 or later. The
  daemon warns at startup when it's off.

  ```json
  "nodeAttrs": [
    { "target": ["autogroup:member"], "attr": ["cache-network-maps"] }
  ]
  ```

## Usage

The CLI asks the local daemon, or another Node's with `--on <node>`. Paths are
`node:path`, or this Node's when the Node is left out. A path on another Node
is absolute or starts with `~`, the home of the user heighliner runs as there.

```sh
heighliner track ~/docs                       # Track a directory as an Item
heighliner sync docs desktop:~/docs           # Add a Replica on another Node
heighliner sync docs nas:/srv/docs --mirror   # One that only receives
heighliner status                             # Each Item's Replicas and their state
heighliner conflicts                          # This Node's Data Conflicts
heighliner conflicts keep docs <version>      # or swap, to make the other side win
heighliner pause docs [node]                  # and resume
heighliner remove-replica docs desktop        # Stop syncing there; files stay
heighliner untrack docs                       # Stop syncing everywhere; files stay
heighliner peripherals                        # Each Node's Bluetooth Peripherals
heighliner peripherals handoff headset desk   # Connect it on desk, off the others
heighliner peripherals pair headset tv        # Pair it with tv; in pairing mode
heighliner ui                                 # Open the web UI
```

Every command has `--help`. Heighliner deletes nothing but expired Displaced
Versions and Data Conflicts: removing a Replica or untracking an Item leaves its
files in place, and `sync` refuses a path that already holds files unless you
pass `--adopt`.

The web UI does everything the CLI does. Open it from any of your devices,
including a phone, at `http://<node>.<tailnet>.ts.net:7130/` or the Node's
Tailscale IP. The short MagicDNS name isn't accepted.

Remote Input turns the web UI into a touchpad and keyboard for a Node, such as
a media PC across the room; you watch its screen, not the page. Turn it on for
each Node that should take it:

```nix
services.heighliner.remoteInput.enable = true;
```

Then open Input on that Node's UI. The page also has each Peripheral's
Handoff buttons and links to the other Nodes' Remote Input. The daemon types
text through a US keyboard layout, so set the desktop's layout to US, and it
types only printable ASCII. It releases every button a page held when the
page's connection drops. The option gives only heighliner's service
`/dev/uinput`, but anything that can reach heighliner from one of your
devices, a program on the media PC included, can then type into any
session on the Node, the login screen included.

A Peripheral is named by its Bluetooth address or by any Node's name for it.
Pairing works only for a Peripheral another Node already has paired, and only
for one that needs no PIN or passkey, such as a headset. Each Node's daemon
reaches BlueZ on the system bus as its user, which BlueZ allows by default; a
Node without BlueZ just lists no Peripherals.

An Item is **Fully Replicated** only once every Replica has been confirmed
current within one connected window; until then, the UI and `status` show the
oldest confirmation instead. Files that sync replaced or deleted are kept in
each Replica's `.stversions`, and the other side of each Data Conflict in its
`.heighliner`, for the Retention Window (30 days by default).

## Partition limits

Heighliner tolerates partitions only as far as Tailscale does:

- A LAN cut off from the internet keeps syncing while its Nodes stay up.
- A Node that restarts during the outage can't reach the others until
  Tailscale's control server is reachable again, or, if it caches network maps
  (see [Tailscale setup](#tailscale-setup)), a DERP relay is.
- There's no LAN fallback outside Tailscale.

Configuration edits are accepted on any Node at any time and merge when Nodes
reconnect; the latest edit of each Item or Replica wins.

## Development

Enter the development shell with `nix develop`, then run:

```sh
just build
just run
just test
just format
just check
```

`just build`, `just run` and `just test` use Cargo. To build or run the Nix
package, use `nix build` or `nix run`.

`just check` checks formatting, runs Clippy and ShellCheck, and runs
`nix flake check` (which builds and tests the package) without rewriting source
files. Formatting and shell checks cover tracked files; stage new files with
`git add` to include them.

Install the formatting hook with `just install-hooks`. It formats fully staged
files and leaves partially staged files alone.

Update Nix dependencies with `nix flake update` and Rust dependencies with
`cargo update`.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
