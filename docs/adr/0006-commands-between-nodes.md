# Peripherals are live Node state, acted on with commands between Nodes

Items and Replicas are records that every Node keeps and merges, so edits work
while Nodes can't reach each other. A Peripheral's connection is different: it
belongs to one Node's Bluetooth stack right now, and moving it needs both
Nodes at once. So heighliner stores nothing about Peripherals. The Node
serving a page asks each reachable Node for its Peripherals. A Handoff,
pairing, disconnecting, forgetting and naming are synchronous requests from
that Node to each Node involved, over write routes in the Node protocol; a
Handoff disconnects wherever the Peripheral is connected, then connects on the
target.

## Considered Options

- **Replicated desired state** (a record naming where each Peripheral should
  be, applied by each Node): a Handoff would happen eventually rather than
  when asked, and a stale record could pull a Peripheral back later.
- **The browser calling each Node itself**: every Node's UI is its own origin,
  so this would need cross-origin requests, which the UI's policy forbids.

## Consequences

The Node protocol, read-only until now, gains routes that change a Node. They
take JSON only, so a page on another site can't send them as a plain form, and
WhoIs authorizes them like every other route. They are added within the
current major version, so a Node running an older heighliner still exchanges
records and just can't manage its Peripherals. A Node that can't be reached
can't be disconnected; the Handoff still tries to connect on the target.
