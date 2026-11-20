{
  lib,
  rustPlatform,
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
        (lib.fileset.fileFilter (file: file.hasExt "md" || file.hasExt "nix") ./.)
        ./flake.lock
        ./justfile
        ./.githooks
        ./.github
      ]
    );
  };

  cargoLock.lockFile = ./Cargo.lock;

  meta.mainProgram = "heighliner";
}
