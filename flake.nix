{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      ...
    }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
    in
    flake-utils.lib.eachSystem supportedSystems (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        package = pkgs.callPackage ./default.nix { };
      in
      {
        packages = {
          "heighliner" = package;
          default = package;
        };

        checks.package = package;

        devShells.default = pkgs.mkShell {
          inputsFrom = [ package ];

          packages = with pkgs; [
            clippy
            just
            nixfmt
            rust-analyzer
            rustfmt
            shfmt
            shellcheck
            syncthing
            bash
          ];
        };

        formatter = pkgs.nixfmt;
      }
    );
}
