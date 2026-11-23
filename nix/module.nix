# Runs heighliner and the Syncthing it drives as one user, reachable only
# over Tailscale, and able to reach nothing else.
{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.heighliner;
  inherit (config.services.tailscale) interfaceName;

  stateDir = "/var/lib/heighliner";
  syncthingHome = "${stateDir}/syncthing";
  apiKeyFile = "${stateDir}/syncthing-api-key";
  socket = "/run/heighliner/syncthing.sock";
  # Syncthing's sync port, which heighliner configures it to listen on.
  syncthingPort = 22000;

  syncthing = lib.getExe cfg.syncthingPackage;

  # Creates the API key and Syncthing's first configuration, hardened
  # before Syncthing ever starts, so it never contacts the internet.
  prepareSyncthing = pkgs.writeShellScript "heighliner-syncthing-prepare" ''
    set -euo pipefail
    umask 077
    if [ ! -s ${apiKeyFile} ]; then
      ${lib.getExe' pkgs.coreutils "head"} -c 32 /dev/urandom \
        | ${lib.getExe' pkgs.coreutils "base64"} \
        | ${lib.getExe' pkgs.coreutils "tr"} -d '/+=\n' >${apiKeyFile}.new
      mv ${apiKeyFile}.new ${apiKeyFile}
    fi
    if [ -e ${syncthingHome} ]; then
      exit 0
    fi
    # Made aside and moved into place, so a start cut short never leaves
    # a configuration that isn't hardened.
    rm -rf ${syncthingHome}.new
    STNODEFAULTFOLDER=1 ${syncthing} generate --home=${syncthingHome}.new --no-port-probing
    config=${syncthingHome}.new/config.xml
    harden() {
      if ! grep -qF "$1" "$config"; then
        echo "can't harden Syncthing's first configuration: it has no $1" >&2
        exit 1
      fi
      sed -i "s|$1|$2|" "$config"
    }
    harden '<globalAnnounceEnabled>true' '<globalAnnounceEnabled>false'
    harden '<localAnnounceEnabled>true' '<localAnnounceEnabled>false'
    harden '<relaysEnabled>true' '<relaysEnabled>false'
    harden '<natEnabled>true' '<natEnabled>false'
    harden '<crashReportingEnabled>true' '<crashReportingEnabled>false'
    harden '<urAccepted>0' '<urAccepted>-1'
    harden '<stunKeepaliveStartS>180' '<stunKeepaliveStartS>0'
    # Until heighliner sets this device's Tailscale addresses.
    harden '<listenAddress>default' '<listenAddress>tcp://127.0.0.1:${toString syncthingPort}'
    mv ${syncthingHome}.new ${syncthingHome}
  '';

  runSyncthing = pkgs.writeShellScript "heighliner-syncthing" ''
    set -euo pipefail
    STGUIAPIKEY=$(<${apiKeyFile})
    export STGUIAPIKEY
    exec ${syncthing} serve \
      --home=${syncthingHome} \
      --gui-address=unix://${socket} \
      --no-browser \
      --no-restart \
      --no-upgrade
  '';

  service = {
    User = cfg.user;
    Group = config.users.users.${cfg.user}.group;
    StateDirectory = "heighliner";
    StateDirectoryMode = "0700";
    Restart = "on-failure";
    RestartSec = 5;
    # Only Tailscale's addresses and this machine; the egress allowlist.
    # The IPv6 range is where headscale and Tailscale give devices their
    # addresses; it also holds Tailscale's 4via6 routes to subnets behind
    # subnet routers, which a deny can't carve out, since allows win.
    IPAddressAllow = [
      "localhost"
      "100.64.0.0/10"
      "fd7a:115c:a1e0::/48"
    ];
    IPAddressDeny = "any";
    RestrictAddressFamilies = [
      "AF_UNIX"
      "AF_INET"
      "AF_INET6"
      "AF_NETLINK"
    ];
    # Not PrivateTmp or PrivateDevices: a Replica may be anywhere the user
    # can write, /tmp and /dev/shm included.
    NoNewPrivileges = true;
    ProtectSystem = "full";
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectKernelLogs = true;
    ProtectControlGroups = true;
    ProtectClock = true;
    ProtectHostname = true;
    RestrictNamespaces = true;
    RestrictRealtime = true;
    RestrictSUIDSGID = true;
    LockPersonality = true;
    SystemCallArchitectures = "native";
  };
in
{
  options.services.heighliner = {
    enable = lib.mkEnableOption "heighliner, which keeps directories replicated across the owner's Tailscale devices with Syncthing";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ../default.nix { };
      defaultText = lib.literalMD "heighliner, built from this flake";
      description = "The heighliner package.";
    };

    syncthingPackage = lib.mkPackageOption pkgs "syncthing" { };

    user = lib.mkOption {
      type = lib.types.str;
      example = "alice";
      description = ''
        The user whose directories this Node replicates. Heighliner and
        Syncthing run as this user, so they can read and write the
        replicas, and only this user's Tailscale devices can use them.
      '';
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 7130;
      description = ''
        The port heighliner serves its web UI, API and Node protocol on,
        on this device's Tailscale addresses. Every Node of a Fleet must
        use the same one.
      '';
    };

    retentionDays = lib.mkOption {
      type = lib.types.ints.between 1 36500;
      default = 30;
      description = ''
        The Retention Window: how many days superseded configuration and
        displaced versions are kept. An item can set its own.
      '';
    };

    pullEvery = lib.mkOption {
      type = lib.types.ints.positive;
      default = 60;
      description = "Seconds between pulls of the other Nodes' configuration.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = config.services.tailscale.enable;
        message = "services.heighliner needs services.tailscale.enable: Nodes only reach each other over Tailscale.";
      }
      {
        assertion = config.users.users ? ${cfg.user};
        message = "services.heighliner.user names ${cfg.user}, who isn't in users.users.";
      }
    ];

    environment.systemPackages = [ cfg.package ];

    # Reachable only over Tailscale.
    networking.firewall.interfaces.${interfaceName}.allowedTCPPorts = [
      cfg.port
      syncthingPort
    ];

    systemd.services.heighliner-syncthing = {
      description = "Syncthing, driven by heighliner";
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" ];
      serviceConfig = service // {
        ExecStartPre = prepareSyncthing;
        ExecStart = runSyncthing;
        RuntimeDirectory = "heighliner";
        RuntimeDirectoryMode = "0700";
      };
    };

    systemd.services.heighliner = {
      description = "heighliner";
      wantedBy = [ "multi-user.target" ];
      wants = [
        "heighliner-syncthing.service"
        "tailscaled.service"
      ];
      after = [
        "heighliner-syncthing.service"
        "tailscaled.service"
      ];
      serviceConfig = service // {
        ExecStart = lib.escapeShellArgs (
          map toString [
            (lib.getExe cfg.package)
            "--port"
            cfg.port
            "daemon"
            "--state-dir"
            stateDir
            "--pull-every"
            cfg.pullEvery
            "--retention-days"
            cfg.retentionDays
            "--syncthing-socket"
            socket
            "--syncthing-api-key-file"
            apiKeyFile
          ]
        );
      };
    };
  };
}
