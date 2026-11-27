# Heighliner is a control plane for the Fleet, not only for its data

Heighliner started as a way to keep directories replicated, but what it built
to do that (Fleet membership by device ownership, WhoIs authorization, the
Node protocol, a UI reachable from any of the owner's devices, and a NixOS
module) is what any feature that spans the owner's machines needs. So features
such as moving a Bluetooth Peripheral between Nodes live in heighliner rather
than in a separate tool that would rebuild all of that.

## Consequences

Replication is one of several features. A feature that needs no Syncthing
still runs in the same daemon, behind the same authorization, on the same port.
