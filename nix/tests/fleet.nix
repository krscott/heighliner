# Two Nodes on a tailnet that headscale runs: an item tracked on one is
# replicated to the other in both directions, each Node's daemon and web UI
# see the Fleet, and Remote Input sent from one reaches the other's kernel.
{ pkgs, module }:

let
  certificates = pkgs.runCommand "headscale-certificates" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
      -out cert.pem -keyout key.pem \
      -subj '/CN=headscale' -addext 'subjectAltName=DNS:headscale'
    mkdir -p $out
    cp cert.pem key.pem $out
  '';

  node = {
    imports = [ module ];
    security.pki.certificateFiles = [ "${certificates}/cert.pem" ];
    services.tailscale = {
      enable = true;
      openFirewall = true;
    };
    services.heighliner = {
      enable = true;
      user = "alice";
      pullEvery = 2;
    };
    users.users.alice.isNormalUser = true;
    environment.systemPackages = [
      pkgs.curl
      pkgs.websocat
    ];
  };
in
pkgs.testers.runNixOSTest {
  name = "heighliner-fleet";

  nodes = {
    alpha = {
      imports = [ node ];
      services.heighliner.remoteInput.enable = true;
    };
    beta = node;

    headscale = {
      services.headscale = {
        enable = true;
        port = 8080;
        settings = {
          server_url = "https://headscale";
          prefixes.v4 = "100.64.0.0/10";
          derp = {
            server = {
              enabled = true;
              region_id = 999;
              stun_listen_addr = "0.0.0.0:3478";
            };
            urls = [ ];
          };
          dns = {
            base_domain = "tailnet";
            override_local_dns = false;
          };
        };
      };
      services.nginx = {
        enable = true;
        virtualHosts.headscale = {
          addSSL = true;
          sslCertificate = "${certificates}/cert.pem";
          sslCertificateKey = "${certificates}/key.pem";
          locations."/" = {
            proxyPass = "http://127.0.0.1:8080";
            proxyWebsockets = true;
          };
        };
      };
      networking.firewall = {
        allowedTCPPorts = [
          80
          443
        ];
        allowedUDPPorts = [ 3478 ];
      };
      environment.systemPackages = [ pkgs.headscale ];
    };
  };

  testScript = ''
    import shlex

    def as_alice(machine, command):
        return machine.succeed(f"su - alice -c {shlex.quote(command)}")

    start_all()
    headscale.wait_for_unit("headscale")
    headscale.wait_for_open_port(443)
    headscale.succeed("headscale users create alice")
    key = headscale.succeed("headscale preauthkeys -u 1 create --reusable").strip()
    for machine in [alpha, beta]:
        machine.succeed(f"tailscale up --login-server https://headscale --auth-key {key}")
    alpha.wait_until_succeeds("tailscale ping beta")

    beta_ip = beta.succeed("tailscale ip -4").strip()
    for machine in [alpha, beta]:
        machine.wait_until_succeeds("journalctl -u heighliner.service | grep -q 'serving the Node protocol'")

    with subtest("Syncthing listens only on the tailnet"):
        beta.wait_until_succeeds(f"ss -ltn | grep -q '{beta_ip}:22000'")
        beta.fail("ss -ltn | grep -E '(0.0.0.0|\\*|\\[::\\]):22000'")

    with subtest("Without network-map caching, the daemon says so"):
        alpha.succeed("journalctl -u heighliner.service | grep -q \"doesn't cache Tailscale's network map\"")

    with subtest("An item tracked on one Node is replicated to the other"):
        as_alice(alpha, "mkdir ~/docs && echo hello >~/docs/hello.txt")
        as_alice(alpha, "heighliner track ~/docs")
        # ~ is beta's user's home, once beta has shared it.
        alpha.wait_until_succeeds(
            "su - alice -c " + shlex.quote("heighliner sync docs beta:~/docs"), timeout=120
        )
        beta.wait_until_succeeds("grep -q hello /home/alice/docs/hello.txt", timeout=120)
        assert beta.succeed("stat -c %U /home/alice/docs/hello.txt").strip() == "alice"

    with subtest("Changes go both ways"):
        as_alice(beta, "echo from beta >~/docs/reply.txt")
        alpha.wait_until_succeeds("grep -q 'from beta' /home/alice/docs/reply.txt", timeout=120)

    with subtest("Both Nodes see the item fully replicated"):
        for machine in [alpha, beta]:
            # The whole line: "not confirmed fully replicated" contains it.
            machine.wait_until_succeeds(
                "su - alice -c 'heighliner status' | grep -qx '  fully replicated'", timeout=120
            )

    with subtest("The web UI answers at the Node's MagicDNS name, from another Node"):
        beta.wait_until_succeeds(
            "curl -sf http://alpha.tailnet:7130/"
            " | grep -qF '<span class=\"badge good\">fully replicated</span>'",
            timeout=60,
        )
        page = beta.succeed("curl -sf http://alpha.tailnet:7130/")
        assert ">docs</a>" in page, page

    with subtest("Remote Input sent from another Node reaches the kernel"):
        name = "heighliner Remote Input"
        event = alpha.wait_until_succeeds(
            "for d in /sys/class/input/event*; do"
            f" grep -qx '{name}' $d/device/name && basename $d; done | grep ."
        ).strip()
        # By path, since systemd-run's PATH lacks the system's packages, and
        # line-buffered, so each event is in the file as it happens.
        alpha.succeed(
            "systemd-run --unit=evtest sh -c"
            f" '${pkgs.coreutils}/bin/stdbuf -oL ${pkgs.evtest}/bin/evtest /dev/input/{event} >/tmp/events'"
        )
        alpha.wait_until_succeeds("grep -q 'Testing ...' /tmp/events", timeout=30)
        messages = [
            '{"type":"text","text":"Hi"}',
            '{"type":"keys","keys":["ctrl","escape"]}',
            '{"type":"move","x":5,"y":-3}',
            '{"type":"button","button":"left","down":true}',
        ]
        url = "ws://alpha.tailnet:7130/ui/input"
        beta.succeed(
            f"printf '%s\\n' {' '.join(shlex.quote(m) for m in messages)}"
            # The URL first: -H takes every argument after it.
            f" | websocat -E {url} -H 'Origin: http://alpha.tailnet:7130'"
        )
        for seen in [
            "(KEY_LEFTSHIFT), value 1",
            "(KEY_H), value 1",
            "(KEY_I), value 0",
            "(KEY_ESC), value 1",
            "(REL_X), value 5",
            "(REL_Y), value -3",
            "(BTN_LEFT), value 1",
            # Released when the connection closed.
            "(BTN_LEFT), value 0",
        ]:
            alpha.wait_until_succeeds(f"grep -qF '{seen}' /tmp/events", timeout=30)

        # Another site's page can't open the connection.
        beta.fail(f"echo '{messages[0]}' | websocat -E {url} -H 'Origin: http://evil.example'")

        page = beta.succeed("curl -sf http://beta.tailnet:7130/input")
        assert "Remote Input is off on this Node" in page, page
        page = beta.succeed("curl -sf http://alpha.tailnet:7130/input/controls")
        assert 'data-socket="/ui/input"' in page, page

    with subtest("A Node that stops is shown unreachable, as last read"):
        beta.succeed("systemctl stop heighliner.service heighliner-syncthing.service")
        alpha.wait_until_succeeds(
            "su - alice -c 'heighliner status' | grep -q 'beta: unreachable'", timeout=60
        )
        status = as_alice(alpha, "heighliner status")
        assert "not confirmed fully replicated" in status, status
  '';
}
