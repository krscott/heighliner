//! Data Conflicts, kept among a Replica's Displaced Versions.
//!
//! When two Replicas change a file concurrently, Syncthing keeps the newer
//! change and renames the other file beside it, as
//! `<stem>.sync-conflict-<date>-<time>-<device><ext>`, where `<device>` is
//! the short ID of the device whose change won. Syncthing can't keep that
//! copy anywhere else, so every folder ignores [`IGNORE_PATTERN`], keeping
//! the copy on the Node it was made on, and heighliner moves it into
//! `.stversions`, Syncthing's versions directory, as
//! `<dir>/<copy's name>/conflict-<seconds>`, where `<seconds>` is when it
//! was moved, since the Unix epoch.
//!
//! The Data Conflicts are those `conflict-` files, so they need no other
//! record. Keeping or swapping one renames the side not chosen to
//! `kept-<seconds>` beside it. Syncthing's versioner ignores both, since
//! they aren't tagged as its versions are, and [`expire`] removes them
//! once they have been there for the Item's Retention Window, as Syncthing
//! does its own versions.
//!
//! Only copies made after heighliner set the Replica up are moved: older
//! ones are the user's files, and stay where they are.
//!
//! Every move is a single rename that never replaces a file. Everything
//! here works on the filesystem and blocks; paths are relative to a
//! Replica's root and use `/`, and symlinks are never followed.

use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustix::fs::{CWD, RenameFlags, renameat_with};

use crate::hlc::Timestamp;

/// The ignore pattern that keeps conflict copies on the Node they were
/// made on.
pub const IGNORE_PATTERN: &str = "*.sync-conflict-*";

const CONFLICT: &str = ".sync-conflict-";

/// Where Syncthing keeps a folder's Displaced Versions, which it ignores.
const VERSIONS: &str = ".stversions";

const MARKER: &str = ".stfolder";

/// A file in the marker, written when heighliner sets a Replica up; its
/// modification time says when.
const SET_UP: &str = "heighliner-set-up";

const UNRESOLVED: &str = "conflict-";
const RESOLVED: &str = "kept-";

/// A conflict between two changes to one file, kept on this Node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataConflict {
    /// The file's path.
    pub path: String,
    /// When the conflict happened, in Syncthing's local-time format,
    /// `YYYYMMDD-HHMMSS`.
    pub at: String,
    /// The short ID of the Syncthing device whose change won.
    pub winner: String,
    /// Its directory within `.stversions`, which identifies it.
    pub version: String,
    /// When its losing side was moved into `.stversions`, from which it
    /// expires.
    pub moved_at: Timestamp,
}

#[derive(Debug)]
pub enum Error {
    /// The Replica's marker is missing, so its disk may not be mounted.
    NotMounted,
    /// A path isn't a plain relative path within the Replica, or goes
    /// through a symlink.
    Unsafe(String),
    /// No Data Conflict has that version.
    NotFound(String),
    /// The file a step would create already exists.
    Exists(PathBuf),
    Io {
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotMounted => f.write_str("its marker is missing; is its disk mounted?"),
            Self::Unsafe(path) => write!(f, "{path} isn't a plain path within the replica"),
            Self::NotFound(version) => write!(f, "there's no data conflict {version}"),
            Self::Exists(path) => write!(f, "{} already exists", path.display()),
            Self::Io { path, source } => write!(f, "{}: {source}", path.display()),
        }
    }
}

impl std::error::Error for Error {}

fn io_error(path: &Path) -> impl FnOnce(io::Error) -> Error + '_ {
    move |source| Error::Io {
        path: path.to_owned(),
        source,
    }
}

/// A conflict copy's name, split.
#[derive(Debug, PartialEq, Eq)]
struct ConflictName<'a> {
    stem: &'a str,
    at: &'a str,
    winner: &'a str,
    ext: &'a str,
}

impl ConflictName<'_> {
    /// The name of the file it is a copy of.
    fn original(&self) -> String {
        format!("{}{}", self.stem, self.ext)
    }
}

/// The length of a short device ID, the first characters of a device ID.
const SHORT_ID: usize = 7;

fn parse(name: &str) -> Option<ConflictName<'_>> {
    let (stem, rest) = name.split_once(CONFLICT)?;
    // `YYYYMMDD-HHMMSS-` then the short ID, all ASCII.
    let bytes = rest.as_bytes();
    let shape = |i: usize, byte: u8| match i {
        8 | 15 => byte == b'-',
        _ => byte.is_ascii_digit(),
    };
    if bytes.len() < 16 + SHORT_ID || !bytes[..16].iter().enumerate().all(|(i, &b)| shape(i, b)) {
        return None;
    }
    let winner = &rest[16..16 + SHORT_ID];
    if !winner
        .bytes()
        .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b))
    {
        return None;
    }
    let ext = &rest[16 + SHORT_ID..];
    if !(ext.is_empty() || ext.starts_with('.')) {
        return None;
    }
    let conflict = ConflictName {
        stem,
        at: &rest[..15],
        winner,
        ext,
    };
    let original = conflict.original();
    let plain = !matches!(original.as_str(), "" | "." | "..") && !original.contains('/');
    plain.then_some(conflict)
}

fn seconds_now() -> u64 {
    Timestamp::now().as_millis() / 1000
}

/// The time in a `conflict-` or `kept-` file's name, if it is one.
fn leaf_time(name: &str, prefix: &str) -> Option<u64> {
    let seconds = name.strip_prefix(prefix)?;
    (!seconds.is_empty() && seconds.bytes().all(|b| b.is_ascii_digit()))
        .then(|| seconds.parse().ok())
        .flatten()
}

/// The unresolved conflict's file in a conflict's directory, and when it
/// was moved there.
fn unresolved(dir: &Path) -> Result<Option<(PathBuf, u64)>, Error> {
    for entry in fs::read_dir(dir).map_err(io_error(dir))? {
        let entry = entry.map_err(io_error(dir))?;
        if let Some(seconds) = entry
            .file_name()
            .to_str()
            .and_then(|n| leaf_time(n, UNRESOLVED))
            && entry.file_type().map_err(io_error(dir))?.is_file()
        {
            return Ok(Some((entry.path(), seconds)));
        }
    }
    Ok(None)
}

/// `relative` as components of a path within a Replica.
fn components(relative: &str) -> Result<Vec<&str>, Error> {
    let unsafe_path = || Error::Unsafe(relative.to_owned());
    let mut parts = Vec::new();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_str().ok_or_else(unsafe_path)?),
            Component::CurDir => {}
            _ => return Err(unsafe_path()),
        }
    }
    Ok(parts)
}

/// The directory `parts` under `root`, checking that each step is a
/// directory rather than a symlink, and creating the missing ones if
/// `create`. `None` if it doesn't exist.
fn directory(root: &Path, parts: &[&str], create: bool) -> Result<Option<PathBuf>, Error> {
    let mut dir = root.to_owned();
    for part in parts {
        dir.push(part);
        loop {
            match fs::symlink_metadata(&dir) {
                Ok(metadata) if metadata.is_dir() => break,
                Ok(_) => return Err(Error::Unsafe(parts.join("/"))),
                Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
                    // Looked at again, in case it was made meanwhile.
                    match fs::create_dir(&dir) {
                        Ok(()) => {}
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(source) => return Err(Error::Io { path: dir, source }),
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(source) => return Err(Error::Io { path: dir, source }),
            }
        }
    }
    Ok(Some(dir))
}

fn check_mounted(root: &Path) -> Result<(), Error> {
    if root.join(MARKER).is_dir() {
        Ok(())
    } else {
        Err(Error::NotMounted)
    }
}

/// Records that heighliner sets up the Replica at `root` now, unless it
/// already has. Its marker must exist.
pub fn mark_set_up(root: &Path) -> io::Result<()> {
    match fs::File::create_new(root.join(MARKER).join(SET_UP)) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

/// When heighliner set up the Replica at `root`, in seconds since the Unix
/// epoch. A Replica set up before this was recorded counts as set up now.
fn set_up_at(root: &Path) -> Result<i64, Error> {
    let path = root.join(MARKER).join(SET_UP);
    mark_set_up(root).map_err(io_error(&path))?;
    let modified = fs::metadata(&path)
        .and_then(|metadata| metadata.modified())
        .map_err(io_error(&path))?;
    let seconds = modified
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(i64::try_from(seconds).unwrap_or(i64::MAX))
}

/// Whether `path` is a regular file, rather than missing; anything else is
/// refused.
fn is_file(path: &Path, shown: &str) -> Result<bool, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(Error::Unsafe(shown.to_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io_error(path)(source)),
    }
}

/// Renames `from` to `to`, unless `to` exists.
fn move_new(from: &Path, to: &Path) -> Result<(), Error> {
    match renameat_with(CWD, from, CWD, to, RenameFlags::NOREPLACE) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::EXIST) => Err(Error::Exists(to.to_owned())),
        // A filesystem without it, such as NFS, can only be checked first.
        Err(rustix::io::Errno::INVAL) if fs::symlink_metadata(to).is_err() => {
            fs::rename(from, to).map_err(io_error(from))
        }
        Err(rustix::io::Errno::INVAL) => Err(Error::Exists(to.to_owned())),
        Err(errno) => Err(io_error(from)(errno.into())),
    }
}

/// What moving conflict copies into Displaced Versions did.
#[derive(Debug, Default)]
pub struct Collected {
    pub found: Vec<DataConflict>,
    /// What couldn't be moved or looked at; those copies stay where they
    /// are, ignored.
    pub failed: Vec<Error>,
}

/// Moves the conflict copies directly in the directory `dir` of the Replica
/// at `root` into its Displaced Versions.
pub fn collect(root: &Path, dir: &str) -> Result<Collected, Error> {
    check_mounted(root)?;
    let since = set_up_at(root)?;
    let mut collected = Collected::default();
    if let Err(error) = collect_into(root, since, dir, &mut collected) {
        collected.failed.push(error);
    }
    Ok(collected)
}

fn collect_into(
    root: &Path,
    since: i64,
    dir: &str,
    collected: &mut Collected,
) -> Result<(), Error> {
    let parts = components(dir)?;
    if parts.first() == Some(&VERSIONS) {
        return Ok(());
    }
    let Some(path) = directory(root, &parts, false)? else {
        return Ok(());
    };
    for entry in fs::read_dir(&path).map_err(io_error(&path))? {
        let entry = entry.map_err(io_error(&path))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if parse(&name).is_none() || !entry.file_type().map_err(io_error(&path))?.is_file() {
            continue;
        }
        // Syncthing renames the losing file to make its copy, which sets
        // its change time; an older copy is the user's.
        let made = entry.metadata().map_err(io_error(&path))?.ctime();
        if made < since {
            continue;
        }
        match displace(root, &parts, &name) {
            Ok(conflict) => collected.found.push(conflict),
            Err(error) => collected.failed.push(error),
        }
    }
    Ok(())
}

/// Moves the conflict copy `name` in the directory `parts` into
/// `.stversions`.
fn displace(root: &Path, parts: &[&str], name: &str) -> Result<DataConflict, Error> {
    let conflict = parse(name).expect("a conflict copy's name");
    let mut dirs = vec![VERSIONS];
    dirs.extend_from_slice(parts);
    dirs.push(name);
    let dir = directory(root, &dirs, true)?.expect("created");
    if let Some((existing, _)) = unresolved(&dir)? {
        return Err(Error::Exists(existing));
    }
    let moved = seconds_now();
    let source = root.join(join(parts, name));
    move_new(&source, &dir.join(format!("{UNRESOLVED}{moved}")))?;
    Ok(DataConflict {
        path: join(parts, &conflict.original()),
        at: conflict.at.to_owned(),
        winner: conflict.winner.to_owned(),
        version: dirs[1..].join("/"),
        moved_at: Timestamp::from_millis(moved * 1000),
    })
}

fn join(parts: &[&str], name: &str) -> String {
    let mut path = parts.join("/");
    if !path.is_empty() {
        path.push('/');
    }
    path.push_str(name);
    path
}

/// Moves every conflict copy in the Replica at `root` into its Displaced
/// Versions.
pub fn sweep(root: &Path) -> Result<Collected, Error> {
    check_mounted(root)?;
    let since = set_up_at(root)?;
    let mut collected = Collected::default();
    let mut dirs = vec![String::new()];
    while let Some(dir) = dirs.pop() {
        let mut visit = || -> Result<(), Error> {
            collect_into(root, since, &dir, &mut collected)?;
            let path = root.join(&dir);
            for entry in fs::read_dir(&path).map_err(io_error(&path))? {
                let entry = entry.map_err(io_error(&path))?;
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                let internal = dir.is_empty() && (name == VERSIONS || name == MARKER);
                if !internal && entry.file_type().map_err(io_error(&path))?.is_dir() {
                    dirs.push(join(&components(&dir)?, &name));
                }
            }
            Ok(())
        };
        if let Err(error) = visit() {
            collected.failed.push(error);
        }
    }
    Ok(collected)
}

/// Visits each conflict's directory in the Replica at `root`'s
/// `.stversions`, with its path there and its copy's name.
fn each_conflict(
    root: &Path,
    mut visit: impl FnMut(&Path, &[&str], &ConflictName<'_>) -> Result<(), Error>,
) -> Result<(), Error> {
    let Some(versions) = directory(root, &[VERSIONS], false)? else {
        return Ok(());
    };
    let mut dirs = vec![(versions, Vec::<String>::new())];
    while let Some((path, parts)) = dirs.pop() {
        for entry in fs::read_dir(&path).map_err(io_error(&path))? {
            let entry = entry.map_err(io_error(&path))?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !entry.file_type().map_err(io_error(&path))?.is_dir() {
                continue;
            }
            let mut inner = parts.clone();
            inner.push(name.clone());
            if let Some(conflict) = parse(&name) {
                let inner: Vec<&str> = inner.iter().map(String::as_str).collect();
                visit(&entry.path(), &inner, &conflict)?;
            } else {
                dirs.push((entry.path(), inner));
            }
        }
    }
    Ok(())
}

fn data_conflict(parts: &[&str], conflict: &ConflictName<'_>, moved: u64) -> DataConflict {
    let (_, dirs) = parts.split_last().expect("a conflict's directory");
    DataConflict {
        path: join(dirs, &conflict.original()),
        at: conflict.at.to_owned(),
        winner: conflict.winner.to_owned(),
        version: parts.join("/"),
        moved_at: Timestamp::from_millis(moved.saturating_mul(1000)),
    }
}

/// The Data Conflicts kept in the Replica at `root`.
pub fn list(root: &Path) -> Result<Vec<DataConflict>, Error> {
    check_mounted(root)?;
    let mut conflicts = Vec::new();
    each_conflict(root, |dir, parts, conflict| {
        if let Some((_, moved)) = unresolved(dir)? {
            conflicts.push(data_conflict(parts, conflict, moved));
        }
        Ok(())
    })?;
    conflicts.sort_by(|a, b| (&a.path, &a.at).cmp(&(&b.path, &b.at)));
    Ok(conflicts)
}

/// Removes the sides of conflicts, resolved or not, that have been in the
/// Replica at `root`'s `.stversions` for longer than `retention` at `now`,
/// and returns how many.
pub fn expire(root: &Path, retention: Duration, now: Timestamp) -> Result<usize, Error> {
    check_mounted(root)?;
    let now = now.as_millis() / 1000;
    let mut removed = 0;
    each_conflict(root, |dir, _, _| {
        for entry in fs::read_dir(dir).map_err(io_error(dir))? {
            let entry = entry.map_err(io_error(dir))?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(moved) = leaf_time(&name, UNRESOLVED).or_else(|| leaf_time(&name, RESOLVED))
            else {
                continue;
            };
            if entry.file_type().map_err(io_error(dir))?.is_file()
                && now.saturating_sub(moved) > retention.as_secs()
            {
                fs::remove_file(entry.path()).map_err(io_error(&entry.path()))?;
                removed += 1;
            }
        }
        // Gone once empty; one still holding something stays.
        let _ = fs::remove_dir(dir);
        Ok(())
    })?;
    Ok(removed)
}

/// The Data Conflict `version` in the Replica at `root`, and its losing
/// side's file.
fn find(root: &Path, version: &str) -> Result<(DataConflict, PathBuf), Error> {
    check_mounted(root)?;
    let not_found = || Error::NotFound(version.to_owned());
    let parts = components(version)?;
    let name = parts.last().ok_or_else(not_found)?;
    let conflict = parse(name).ok_or_else(not_found)?;
    let mut path = vec![VERSIONS];
    path.extend_from_slice(&parts);
    let dir = directory(root, &path, false)?.ok_or_else(not_found)?;
    let (loser, moved) = unresolved(&dir)?.ok_or_else(not_found)?;
    Ok((data_conflict(&parts, &conflict, moved), loser))
}

/// Where the side not chosen of the conflict whose losing side is `loser`
/// is kept from now on.
fn kept(loser: &Path) -> PathBuf {
    loser.with_file_name(format!("{RESOLVED}{}", seconds_now()))
}

/// Keeps the winning change of the Data Conflict `version`.
pub fn keep(root: &Path, version: &str) -> Result<DataConflict, Error> {
    let (conflict, loser) = find(root, version)?;
    move_new(&loser, &kept(&loser))?;
    Ok(conflict)
}

/// Makes the losing change of the Data Conflict `version` the file's
/// content again, keeping the content it replaces. The two are exchanged
/// in one rename, so the file is never missing, and a change made to it
/// meanwhile is what gets kept rather than lost.
pub fn swap(root: &Path, version: &str) -> Result<DataConflict, Error> {
    let (conflict, loser) = find(root, version)?;
    let kept = kept(&loser);
    if fs::symlink_metadata(&kept).is_ok() {
        return Err(Error::Exists(kept));
    }
    let parts = components(&conflict.path)?;
    let (name, dirs) = parts.split_last().expect("a file's path has a name");
    let live_dir =
        directory(root, dirs, false)?.ok_or_else(|| Error::Unsafe(conflict.path.clone()))?;
    let live = live_dir.join(name);
    // Newer than the change it replaces, and changed enough for Syncthing
    // to look at its content again.
    fs::File::options()
        .write(true)
        .open(&loser)
        .and_then(|file| file.set_modified(SystemTime::now()))
        .map_err(io_error(&loser))?;
    if !is_file(&live, &conflict.path)? {
        move_new(&loser, &live)?;
        return Ok(conflict);
    }
    renameat_with(CWD, &loser, CWD, &live, RenameFlags::EXCHANGE)
        .map_err(|errno| io_error(&live)(errno.into()))?;
    // `loser` now holds what was the file's content.
    move_new(&loser, &kept)?;
    Ok(conflict)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join(MARKER)).unwrap();
        mark_set_up(dir.path()).unwrap();
        // Copies made from here on are newer than the set-up.
        let past = SystemTime::now() - Duration::from_secs(60);
        fs::File::options()
            .write(true)
            .open(dir.path().join(MARKER).join(SET_UP))
            .unwrap()
            .set_modified(past)
            .unwrap();
        dir
    }

    fn write(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn read(root: &Path, path: &str) -> String {
        fs::read_to_string(root.join(path)).unwrap()
    }

    /// The files in a conflict's directory.
    fn leaves(root: &Path, version: &str) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(root.join(VERSIONS).join(version))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    fn only_leaf(root: &Path, version: &str, prefix: &str) -> String {
        let leaves = leaves(root, version);
        assert_eq!(leaves.len(), 1, "{leaves:?}");
        assert!(leaves[0].starts_with(prefix), "{leaves:?}");
        format!("{VERSIONS}/{version}/{}", leaves[0])
    }

    const COPY: &str = "notes.sync-conflict-20261121-073910-ABCDEFG.txt";

    #[test]
    fn conflict_copy_names_parse() {
        assert_eq!(
            parse(COPY),
            Some(ConflictName {
                stem: "notes",
                at: "20261121-073910",
                winner: "ABCDEFG",
                ext: ".txt",
            })
        );
        let original = |name| parse(name).map(|conflict| conflict.original());
        for (copy, name) in [
            (
                "archive.tar.sync-conflict-20261121-073910-ABCDEFG.gz",
                "archive.tar.gz",
            ),
            ("Makefile.sync-conflict-20261121-073910-ABCDEFG", "Makefile"),
            (".sync-conflict-20261121-073910-ABCDEFG.bashrc", ".bashrc"),
            (
                "trailing.sync-conflict-20261121-073910-ABCDEFG.",
                "trailing.",
            ),
            (
                "tilde.sync-conflict-20261121-073910-ABCDEFG.txt~backup",
                "tilde.txt~backup",
            ),
        ] {
            assert_eq!(original(copy).as_deref(), Some(name), "{copy}");
        }
        for name in [
            "notes.txt",
            "notes.sync-conflict-2026-ABCDEFG.txt",
            // Not ASCII where the time goes.
            "notes.sync-conflict-1234567é123456-ABCDEFG.txt",
            "notes.sync-conflict-20261121-073910-abcdefg.txt",
            "notes.sync-conflict-20261121-073910-ABCDEFGH.txt",
            // No name left once the conflict's part is taken out.
            ".sync-conflict-20261121-073910-ABCDEFG",
            ".sync-conflict-20261121-073910-ABCDEFG.",
        ] {
            assert_eq!(parse(name), None, "{name}");
        }
    }

    #[test]
    fn conflict_copies_move_into_displaced_versions() {
        let root = replica();
        let root = root.path();
        write(root, &format!("docs/{COPY}"), "lost");
        write(root, "docs/notes.txt", "won");
        let found = collect(root, "docs").unwrap().found;
        let [conflict] = &found[..] else {
            panic!("{found:?}");
        };
        assert_eq!(conflict.path, "docs/notes.txt");
        assert_eq!(conflict.at, "20261121-073910");
        assert_eq!(conflict.winner, "ABCDEFG");
        assert_eq!(conflict.version, format!("docs/{COPY}"));
        assert!(!root.join("docs").join(COPY).exists());
        let loser = only_leaf(root, &conflict.version, UNRESOLVED);
        assert_eq!(read(root, &loser), "lost");
        assert_eq!(list(root).unwrap(), found);
        // Ordinary versions aren't conflicts.
        write(root, ".stversions/docs/notes~20261120-000000.txt", "older");
        assert_eq!(list(root).unwrap(), found);
    }

    #[test]
    fn copies_older_than_the_replica_stay_where_they_are() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "the user's");
        // Set up after the copy was made.
        let marker = root.join(MARKER).join(SET_UP);
        fs::File::options()
            .write(true)
            .open(&marker)
            .unwrap()
            .set_modified(SystemTime::now() + Duration::from_secs(60))
            .unwrap();
        let swept = sweep(root).unwrap();
        assert!(swept.found.is_empty() && swept.failed.is_empty());
        assert_eq!(read(root, COPY), "the user's");
    }

    #[test]
    fn a_long_name_still_fits() {
        let root = replica();
        let root = root.path();
        // The longest name a copy can have on most filesystems.
        let copy = format!("{}{}", "x".repeat(255 - COPY.len()), COPY);
        write(root, &copy, "lost");
        let collected = collect(root, "").unwrap();
        assert!(collected.failed.is_empty(), "{:?}", collected.failed);
        assert_eq!(list(root).unwrap().len(), 1);
    }

    #[test]
    fn sweeps_find_nested_copies_without_following_symlinks() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "top");
        write(root, &format!("a/b/{COPY}"), "nested");
        let outside = tempfile::tempdir().unwrap();
        write(outside.path(), COPY, "outside");
        std::os::unix::fs::symlink(outside.path(), root.join("link")).unwrap();
        let swept = sweep(root).unwrap();
        assert!(swept.failed.is_empty(), "{:?}", swept.failed);
        let mut found: Vec<String> = swept.found.into_iter().map(|c| c.path).collect();
        found.sort();
        assert_eq!(found, ["a/b/notes.txt", "notes.txt"]);
        assert!(outside.path().join(COPY).exists());
        let through_link = collect(root, "link").unwrap();
        assert!(matches!(through_link.failed[..], [Error::Unsafe(_)]));
    }

    #[test]
    fn copies_never_move_through_a_symlinked_versions_directory() {
        let root = replica();
        let root = root.path();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join(VERSIONS)).unwrap();
        write(root, COPY, "lost");
        let collected = collect(root, "").unwrap();
        assert!(matches!(collected.failed[..], [Error::Unsafe(_)]));
        assert_eq!(read(root, COPY), "lost");
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
    }

    #[test]
    fn keeping_leaves_the_loser_beside_it() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "lost");
        write(root, "notes.txt", "won");
        let version = collect(root, "").unwrap().found.remove(0).version;
        keep(root, &version).unwrap();
        assert!(list(root).unwrap().is_empty());
        assert_eq!(read(root, &only_leaf(root, &version, RESOLVED)), "lost");
        assert_eq!(read(root, "notes.txt"), "won");
        assert!(matches!(keep(root, &version), Err(Error::NotFound(_))));
    }

    #[test]
    fn swapping_exchanges_the_loser_and_the_winner() {
        let root = replica();
        let root = root.path();
        write(root, &format!("docs/{COPY}"), "lost");
        write(root, "docs/notes.txt", "won");
        let version = collect(root, "docs").unwrap().found.remove(0).version;
        let before = SystemTime::now() - Duration::from_secs(1);
        swap(root, &version).unwrap();
        assert!(list(root).unwrap().is_empty());
        assert_eq!(read(root, "docs/notes.txt"), "lost");
        let modified = fs::metadata(root.join("docs/notes.txt"))
            .unwrap()
            .modified()
            .unwrap();
        assert!(modified >= before);
        assert_eq!(read(root, &only_leaf(root, &version, RESOLVED)), "won");
        assert_eq!(fs::read_dir(root.join("docs")).unwrap().count(), 1);
    }

    #[test]
    fn swapping_restores_a_deleted_file() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "lost");
        let version = collect(root, "").unwrap().found.remove(0).version;
        swap(root, &version).unwrap();
        assert_eq!(read(root, "notes.txt"), "lost");
        assert!(list(root).unwrap().is_empty());
    }

    #[test]
    fn sides_expire_from_when_they_were_moved() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "lost");
        write(root, "notes.txt", "won");
        let conflict = collect(root, "").unwrap().found.remove(0);
        let window = Duration::from_secs(30 * 24 * 60 * 60);
        let moved = conflict.moved_at;
        // An old conflict copy isn't expired by its name's time.
        assert_eq!(
            expire(root, window, moved.saturating_add(window)).unwrap(),
            0
        );
        assert_eq!(list(root).unwrap().len(), 1);
        let later = moved.saturating_add(window + Duration::from_secs(1));
        assert_eq!(expire(root, window, later).unwrap(), 1);
        assert!(list(root).unwrap().is_empty());
        assert!(!root.join(VERSIONS).join(&conflict.version).exists());
        // Syncthing's own versions are left to Syncthing.
        write(root, ".stversions/notes~20200101-000000.txt", "old");
        assert_eq!(expire(root, window, later).unwrap(), 0);
        assert!(root.join(".stversions/notes~20200101-000000.txt").exists());
    }

    #[test]
    fn nothing_is_overwritten() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "lost");
        write(root, "notes.txt", "won");
        let version = collect(root, "").unwrap().found.remove(0).version;
        // An unresolved conflict with the same copy's name.
        write(root, COPY, "again");
        let collected = collect(root, "").unwrap();
        assert!(matches!(collected.failed[..], [Error::Exists(_)]));
        assert_eq!(read(root, COPY), "again");
        assert_eq!(list(root).unwrap().len(), 1);
        assert_eq!(read(root, "notes.txt"), "won");
        assert_eq!(list(root).unwrap()[0].version, version);
    }

    #[test]
    fn paths_must_stay_within_the_replica() {
        let root = replica();
        let root = root.path();
        let outside = collect(root, "../x").unwrap();
        assert!(matches!(outside.failed[..], [Error::Unsafe(_)]));
        assert!(matches!(keep(root, "/etc/passwd"), Err(Error::Unsafe(_))));
        // A Replica whose disk isn't mounted is left alone.
        let unmounted = tempfile::tempdir().unwrap();
        write(unmounted.path(), COPY, "lost");
        assert!(matches!(sweep(unmounted.path()), Err(Error::NotMounted)));
        assert!(unmounted.path().join(COPY).exists());
    }
}
