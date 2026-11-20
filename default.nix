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
    fileset = lib.fileset.unions [
      ./Cargo.toml
      ./Cargo.lock
      (lib.fileset.fileFilter (file: file.hasExt "rs") ./.)
    ];
  };

  cargoLock.lockFile = ./Cargo.lock;

  meta.mainProgram = "heighliner";
}
