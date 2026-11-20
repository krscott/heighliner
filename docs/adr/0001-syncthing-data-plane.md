# Syncthing is the data plane; heighliner is the control plane

Heighliner does not move bytes itself. Each Node runs a dedicated Syncthing
instance that heighliner owns completely: heighliner generates its whole
configuration and drives it through the REST API, and overwrites any manual
edits. Writing a bidirectional sync engine that tolerates partitions (rename
detection, partial transfers, deletion propagation, conflicts) costs far more
than the fleet-level features heighliner exists to add.

Syncthing contacts public infrastructure by default: discovery, relays, STUN,
NAT traversal, crash reporting and release checks. Heighliner keeps all of its
traffic on the tailnet in two layers:

- **Configuration**: global and local discovery, relays, NAT traversal, STUN
  and crash reporting are disabled, usage reporting is declined, Syncthing
  listens only on the Node's Tailscale address, and peers are configured with
  static Tailscale addresses. The binary is built with upgrades disabled.
- **Enforcement**: Syncthing runs as a systemd system service whose IP egress
  is limited to the tailnet ranges and localhost (`IPAddressDeny=any` plus
  `IPAddressAllow=`). The kernel enforces this whatever Syncthing attempts, so
  a missed setting or a new upstream feature cannot leak traffic.

## Consequences

The enforcement layer needs a systemd system service, which is why Fleet
membership starts with NixOS. A platform that cannot enforce tailnet-only
egress cannot join the Fleet.

Partition tolerance is bounded by Tailscale's. A LAN with no internet keeps
working while its Nodes stay up. Nodes that restart during the outage cannot
reach each other until a DERP relay or the coordination server is reachable
again, because each restart creates a new disco key that only those services
can distribute. Syncthing does not fall back to LAN addresses. If that ever
becomes necessary, the narrowest fix is to add LAN peer addresses and widen
the egress allowlist to the trusted home subnets for Syncthing only.
