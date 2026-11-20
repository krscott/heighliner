# heighliner

A control plane that tracks directories across a Tailscale fleet and keeps them
replicated with Syncthing. See [GLOSSARY.md](GLOSSARY.md) for the vocabulary
and [docs/adr](docs/adr) for the foundational decisions.

Heighliner is under development and not usable yet.

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
