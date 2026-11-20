# Fleet membership comes from device ownership, not a Tailscale tag

A Node is a tailnet device owned by the user that runs heighliner. Every
request between Nodes, and every UI request, is authorized by checking that
Tailscale's WhoIs returns that user. A tag such as `tag:heighliner` would be
the usual way to mark members, but tagging a device moves its ownership from
the user to the tag. That would change how ACLs, Tailscale SSH and Taildrop
treat the user's desktops, and WhoIs would stop returning the user.

## Considered Options

- **Tag**: rejected because of the ownership change described above.
- **Grant with an app capability**: rejected because it needs tailnet policy
  setup in exchange for no benefit at this scale.
