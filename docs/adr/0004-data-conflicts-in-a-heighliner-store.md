# Data Conflicts are kept in a heighliner store in each Replica

Syncthing leaves a conflict's losing side beside the file, where it syncs to
every Replica, and can only delete it otherwise (`maxConflicts=0`). Heighliner
wants it kept, out of the tree, on the Node whose change lost. So every folder
ignores conflict copies and a `.heighliner` directory at its root, Syncthing
keeps every copy (`maxConflicts=-1`), and heighliner moves each new copy into
`.heighliner/conflicts`, where it lists them, keeps or swaps them, and expires
them itself, a Retention Window after the move.

## Considered Options

- **Syncthing's `.stversions`**, where its other Displaced Versions go: its
  cleaner reads a version's time from anywhere in its path, so a `~<date>` in a
  file's name could expire a fresh conflict, and it can't tell heighliner's
  files from the user's.
- **A directory outside the tree**, such as the daemon's state directory: a
  move there can cross filesystems, copying large files, and Syncthing's user
  would need to be able to write there.

## Consequences

Each Replica gains a `.heighliner` directory that heighliner owns; one it didn't
make is the user's, so a path holding one is refused. Only conflict copies made
after heighliner set a Replica up are moved, since older ones are the user's
files.
