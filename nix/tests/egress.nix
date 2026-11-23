# One Node, not yet on a tailnet, and a host outside it: the machine can
# reach the host, but neither of heighliner's services can, and the first
# Syncthing configuration is hardened.
{ pkgs, module }:

pkgs.testers.runNixOSTest {
  name = "heighliner-egress";

  nodes = {
    machine = {
      imports = [ module ];
      services.tailscale.enable = true;
      services.heighliner = {
        enable = true;
        user = "alice";
      };
      users.users.alice.isNormalUser = true;
      environment.systemPackages = [
        pkgs.curl
        # Runs a command inside a unit's cgroup, under its IP filters.
        (pkgs.writeShellScriptBin "in-unit" ''
          set -euo pipefail
          cgroup=$(systemctl show --property=ControlGroup --value "$1")
          if [ -z "$cgroup" ]; then
            echo "$1 isn't running" >&2
            exit 1
          fi
          shift
          exec sh -c 'echo $$ >"$0/cgroup.procs" && exec "$@"' "/sys/fs/cgroup$cgroup" "$@"
        '')
      ];
    };

    outside = {
      services.nginx = {
        enable = true;
        virtualHosts.outside.locations."/".return = "200 outside";
      };
      networking.firewall.allowedTCPPorts = [ 80 ];
    };
  };

  testScript = ''
    start_all()
    outside.wait_for_open_port(80)
    machine.wait_for_unit("heighliner-syncthing.service")
    machine.wait_until_succeeds("test -S /run/heighliner/syncthing.sock")
    machine.wait_for_unit("heighliner.service")

    with subtest("Syncthing's first configuration is hardened"):
        config = machine.succeed("cat /var/lib/heighliner/syncthing/config.xml")
        for setting in [
            "<globalAnnounceEnabled>false",
            "<localAnnounceEnabled>false",
            "<relaysEnabled>false",
            "<natEnabled>false",
            "<crashReportingEnabled>false",
            "<urAccepted>-1",
            "<stunKeepaliveStartS>0",
            "<listenAddress>tcp://127.0.0.1:22000",
        ]:
            assert setting in config, setting

    with subtest("Only the user can read the state"):
        machine.succeed("test \"$(stat -c '%U %a' /var/lib/heighliner)\" = 'alice 700'")
        machine.succeed("test \"$(stat -c '%U %a' /var/lib/heighliner/syncthing-api-key)\" = 'alice 600'")
        machine.succeed("test \"$(stat -c '%U' /run/heighliner/syncthing.sock)\" = alice")

    with subtest("The services reach this machine, but nothing outside the tailnet"):
        machine.succeed("curl -sf --max-time 5 http://outside/")
        outside6 = outside.succeed("ip -6 -o addr show dev eth1 scope global | awk '{print $4}' | cut -d/ -f1").strip()
        machine.succeed(f"curl -sf --max-time 5 'http://[{outside6}]/'")
        machine.succeed("${pkgs.python3}/bin/python3 -m http.server --bind 127.0.0.1 8000 >/dev/null 2>&1 &")
        machine.wait_for_open_port(8000)
        for unit in ["heighliner-syncthing.service", "heighliner.service"]:
            machine.succeed(f"in-unit {unit} curl -sf --max-time 5 http://127.0.0.1:8000/")
            # The filter drops the packets, so curl times out, where the
            # machine itself reached the same host.
            for url in ["http://outside/", f"http://[{outside6}]/"]:
                code, _ = machine.execute(f"in-unit {unit} curl -gsf --max-time 3 '{url}'")
                assert code == 28, f"{unit}: curl {url} exited {code}"

    with subtest("The ports are open only on Tailscale's interface"):
        rules = machine.succeed("iptables -S nixos-fw")
        for port in ["7130", "22000"]:
            opened = [rule for rule in rules.splitlines() if f"--dport {port} " in rule]
            assert opened, rules
            assert all("-i tailscale0" in rule for rule in opened), opened

    with subtest("The daemon waits for Tailscale instead of failing"):
        machine.sleep(5)
        machine.succeed("systemctl is-active heighliner.service")
        machine.wait_until_succeeds("journalctl -u heighliner.service | grep -q 'retrying'")
  '';
}
