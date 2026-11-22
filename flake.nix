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
    {
      nixosModules.default = ./nix/module.nix;
    }
    // flake-utils.lib.eachSystem supportedSystems (
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

        # NixOS VM tests, apart from `checks` since they need KVM and take
        # minutes: `just vm-test`.
        legacyPackages.vmTests = {
          egress = import ./nix/tests/egress.nix {
            inherit pkgs;
            module = ./nix/module.nix;
          };
        };

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
