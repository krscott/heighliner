{
  lib,
  rustPlatform,
  syncthing,
}:

let
  cargoToml = lib.importTOML ./Cargo.toml;
in
rustPlatform.buildRustPackage {
  pname = cargoToml.package.name;
  inherit (cargoToml.package) version;

  src = lib.fileset.toSource {
    root = ./.;
    # Exclude only files the build can't use, so new build inputs are
    # included by default.
    fileset = lib.fileset.difference ./. (
      lib.fileset.unions [
        ./.githooks
        ./.github
        ./GLOSSARY.md
        ./README.md
        ./default.nix
        ./docs
        ./flake.lock
        ./flake.nix
        ./justfile
        ./nix
      ]
    );
  };

  cargoLock.lockFile = ./Cargo.lock;

  # The Syncthing driver's tests run real Syncthing instances.
  nativeCheckInputs = [ syncthing ];

  meta.mainProgram = "heighliner";
}
