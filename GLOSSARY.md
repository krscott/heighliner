# Heighliner

Heighliner is a control plane for the user's Tailscale fleet. It keeps data
replicated between nodes, continuing to work while LANs are cut off from each
other, moves Bluetooth peripherals between them, and lets the UI act as a
node's keyboard and pointer.

## Fleet

**Fleet**:
The set of tailnet devices owned by the user that run heighliner.
_Avoid_: Cluster, network, tailnet (the tailnet may contain non-members)

**Node**:
One member of the Fleet, identified by its Tailscale identity.
_Avoid_: Host, peer, device, machine

**Departed Node**:
A former Node that has left the tailnet or was explicitly removed from the
Fleet. Unlike an offline Node, it is not expected to come back.
_Avoid_: Removed node, dead node

## Data

**Item**:
A directory tree that the Fleet tracks, with a permanent identity and an
editable name; names are not guaranteed unique. It may exclude some of its
contents, which is also how a single file is tracked.
_Avoid_: Tracked Item, share, folder, resource, sync

**Replica**:
One copy of an Item, at a specific path on a specific Node.
_Avoid_: Location, copy, mirror (mirror is a Role)

**Role**:
How a Replica takes part in its Item: a **source** only sends changes, a
**mirror** only receives them, and a **peer** does both.
_Avoid_: Mode, direction

**Direction**:
Whether an Item is **one-way** (one source; every other Replica is a mirror)
or **two-way** (one or more peers and any number of mirrors).
_Avoid_: Sync mode, sync type

## Replication state

**Replica State**:
What a Replica knows about its own freshness: **current** (has everything its
reachable senders have), **syncing**, **behind** (its senders are unreachable;
last confirmed current at a known time), **error**, or **paused**.
_Avoid_: Sync status, health

**Fully Replicated**:
An Item whose every Replica was confirmed current within one connected window.
_Avoid_: In sync, synced (ambiguous when part of the Fleet is unreachable)

## Conflicts and history

**Data Conflict**:
A file changed on two Replicas without either change having seen the other, for
example concurrent edits, or an edit on one Replica and a delete on another.
_Avoid_: Merge conflict, collision

**Displaced Version**:
A file's previous content that sync replaced or deleted, kept on the Node where
it was displaced.
_Avoid_: Backup, snapshot, old file

**Retention Window**:
How long Displaced Versions and the losing side of a Data Conflict are kept and
can be restored.
_Avoid_: TTL, grace period

## Peripherals

**Peripheral**:
A Bluetooth device paired with one or more Nodes, identified by its Bluetooth
address.
_Avoid_: Device, Bluetooth device, accessory

**Handoff**:
Moving a Peripheral's connection from whichever Nodes it is connected to onto
one other Node.
_Avoid_: Transfer, move, switch

## Input

**Remote Input**:
Keyboard and pointer input sent from the UI to a Node, as if from devices
plugged into it.
_Avoid_: Remote control, remote desktop (implies the screen), KVM, virtual keyboard

## Relationships

- An **Item** has one or more **Replicas**; each **Replica** is on exactly one **Node** and has exactly one **Role**.
- Tracking a path creates an **Item** with its first **Replica**; syncing it somewhere adds another **Replica**.
- An **Item**'s **Direction** limits which **Roles** its **Replicas** may have: a source exists only in a one-way **Item**, and a peer only in a two-way one.
- The **Replicas** of a **Departed Node** stay until they are removed explicitly.
- Removing a **Replica** or untracking an **Item** never deletes files.
- Every data movement is ongoing replication between **Replicas**; there is no one-shot copy or move.
- A **Data Conflict** is resolved by keeping one side and making the other a **Displaced Version**, which can be reversed within the **Retention Window**.
- A **Peripheral** can be handed off only to a **Node** it is paired with; a **Handoff** leaves it connected to that **Node** alone.
- A **Peripheral** may have a different name on each **Node** until one name is chosen for all of them.
- A **Node** takes **Remote Input** only once it is enabled on that **Node**.
