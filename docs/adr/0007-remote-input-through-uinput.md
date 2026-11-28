# Remote Input goes through uinput on the target Node

Remote Input reaches a Node's desktop through a virtual keyboard and pointer
that the daemon creates with the kernel's uinput, so the desktop sees them as
devices plugged into the Node. The daemon often runs as a different user from
the one whose session is on the screen, such as a media Node that logs in a
separate user automatically, and uinput works whoever owns the screen, at the
login screen too, and whatever the compositor.

The Remote Input page is served by the Node it controls, and other Nodes'
pages link to it, as they do for Data Conflicts. Input goes straight from the
browser to that Node over one connection.

## Considered Options

- **The desktop's remote-input portal (libei)**: it would have to run inside
  the session user's desktop rather than in the daemon, asks for consent on
  the screen, and depends on the compositor supporting it.
- **Relaying input through the Node that served the page**: every pointer
  move would take an extra network hop, and the Node protocol would need
  routes that inject input.

## Consequences

A Node that enables Remote Input gives the daemon write access to
`/dev/uinput`, which can type into any session on the machine, so it's off
by default and enabled per Node. WhoIs authorizes the owner's devices, not
programs on them, so any program on any of them, including another
user's on the Node itself, can send it. Input follows the keyboard layout the
desktop has, so the daemon types text through one layout, US, and the page
refuses characters it can't type.
