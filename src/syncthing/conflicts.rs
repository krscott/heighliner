//! Data Conflicts, kept among a Replica's Displaced Versions.
//!
//! When two Replicas change a file concurrently, Syncthing keeps the newer
//! change and renames the other file beside it, as
//! `<stem>.sync-conflict-<date>-<time>-<device><ext>`, where `<device>` is
//! the short ID of the device whose change won. Syncthing can't keep that
//! copy anywhere else, so every folder ignores [`IGNORE_PATTERN`], keeping
//! the copy on the Node it was made on, and heighliner moves it into its
//! own directory in the Replica, [`STORE`], which every folder ignores too:
//! `.heighliner/conflicts/<dir>/<copy's name>/conflict-<seconds>`, where
//! `<seconds>` is when it was moved, since the Unix epoch. Syncthing never
//! looks there, so only heighliner moves or removes what is there.
//!
//! The Data Conflicts are those `conflict-` files, so they need no other
//! record. Keeping or swapping one renames the side not chosen to
//! `kept-<seconds>` beside it (and while a swap is under way, the losing
//! side is `swapping-<seconds>`). [`expire`] removes each of these once it
//! has been there for the Item's Retention Window, as Syncthing does its
//! own Displaced Versions.
//!
//! Only copies made after heighliner set the Replica up are moved: older
//! ones are the user's files, and stay where they are.
//!
//! Every move is a single rename that never replaces a file, and one
//! operation runs at a time. Everything here works on the filesystem and
//! blocks; paths are relative to a Replica's root and use `/`, and
//! symlinks are never followed.

use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustix::fs::{CWD, RenameFlags, renameat_with};

use crate::hlc::Timestamp;

/// The ignore pattern that keeps conflict copies on the Node they were
/// made on.
pub const IGNORE_PATTERN: &str = "*.sync-conflict-*";

/// Heighliner's directory in each Replica.
pub const STORE: &str = ".heighliner";

/// The ignore pattern that keeps [`STORE`] out of Syncthing's sight.
pub const STORE_PATTERN: &str = "/.heighliner";

const CONFLICTS: &str = "conflicts";

/// A file in [`STORE`], written when heighliner sets a Replica up; its
/// modification time says when.
const SET_UP: &str = "set-up";

const CONFLICT: &str = ".sync-conflict-";
const MARKER: &str = ".stfolder";
const VERSIONS: &str = ".stversions";

const UNRESOLVED: &str = "conflict-";
const SWAPPING: &str = "swapping-";
const RESOLVED: &str = "kept-";

/// How far a conflict copy's name's local time can be from UTC.
const MAX_UTC_OFFSET: Duration = Duration::from_secs(14 * 60 * 60);

/// Held for each operation, so that two never act on one conflict.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn one_at_a_time() -> MutexGuard<'static, ()> {
    ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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
    /// Its directory within `.heighliner/conflicts`, which identifies it.
    pub version: String,
    /// When its losing side was moved there, from which it expires.
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

    /// Its time, read as UTC, in seconds since the Unix epoch. It is local
    /// time, so it is up to [`MAX_UTC_OFFSET`] from the true time.
    fn at_as_utc(&self) -> i64 {
        let number = |range: std::ops::Range<usize>| -> i64 {
            self.at[range].parse().expect("checked to be digits")
        };
        let days = days_from_civil(number(0..4), number(4..6), number(6..8));
        days * 86_400 + number(9..11) * 3600 + number(11..13) * 60 + number(13..15)
    }
}

/// Days since 1970-01-01 of a date in the proleptic Gregorian calendar
/// (Howard Hinnant's days-from-civil algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The length of a short device ID, the first characters of a device ID.
const SHORT_ID: usize = 7;

fn parse(name: &str) -> Option<ConflictName<'_>> {
    let (stem, rest) = name.split_once(CONFLICT)?;
    // `YYYYMMDD-HHMMSS-` then the short ID, all ASCII, so the slices
    // below fall on character boundaries.
    let bytes = rest.as_bytes();
    let shape = |i: usize, byte: u8| match i {
        8 | 15 => byte == b'-',
        0..16 => byte.is_ascii_digit(),
        _ => byte.is_ascii_uppercase() || (b'2'..=b'7').contains(&byte),
    };
    let end = 16 + SHORT_ID;
    if bytes.len() < end || !bytes[..end].iter().enumerate().all(|(i, &b)| shape(i, b)) {
        return None;
    }
    let ext = &rest[end..];
    if !(ext.is_empty() || ext.starts_with('.')) {
        return None;
    }
    let conflict = ConflictName {
        stem,
        at: &rest[..15],
        winner: &rest[16..end],
        ext,
    };
    let original = conflict.original();
    let plain = !matches!(original.as_str(), "" | "." | "..") && !original.contains('/');
    plain.then_some(conflict)
}

fn seconds_now() -> u64 {
    Timestamp::now().as_millis() / 1000
}

/// The time in a leaf's name with `prefix`, if it has it.
fn leaf_time(name: &str, prefix: &str) -> Option<u64> {
    let seconds = name.strip_prefix(prefix)?;
    if seconds.is_empty() || !seconds.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    seconds.parse().ok()
}

/// The unresolved conflict's file in a conflict's directory, and when it
/// was moved there.
fn unresolved(dir: &Path) -> Result<Option<(PathBuf, u64)>, Error> {
    for entry in fs::read_dir(dir).map_err(io_error(dir))? {
        let entry = entry.map_err(io_error(dir))?;
        if let Some(seconds) = entry
            .file_name()
            .to_str()
            .and_then(|name| leaf_time(name, UNRESOLVED))
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

/// The directory `parts` in the conflicts' store.
fn stored(root: &Path, parts: &[&str], create: bool) -> Result<Option<PathBuf>, Error> {
    let mut all = vec![STORE, CONFLICTS];
    all.extend_from_slice(parts);
    directory(root, &all, create)
}

fn check_mounted(root: &Path) -> Result<(), Error> {
    if root.join(MARKER).is_dir() {
        Ok(())
    } else {
        Err(Error::NotMounted)
    }
}

/// Records that heighliner sets up the Replica at `root` now.
pub fn mark_set_up(root: &Path) -> Result<(), Error> {
    let _one = one_at_a_time();
    mark(root)
}

fn mark(root: &Path) -> Result<(), Error> {
    let store = directory(root, &[STORE], true)?.expect("created");
    let path = store.join(SET_UP);
    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => return Err(io_error(&path)(source)),
    }
    fs::File::create_new(&path)
        .map(drop)
        .map_err(io_error(&path))
}

/// When heighliner set up the Replica at `root`. A Replica set up before
/// this was recorded counts as set up now.
fn set_up_at(root: &Path) -> Result<SystemTime, Error> {
    let path = root.join(STORE).join(SET_UP);
    let metadata = match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            mark(root)?;
            fs::symlink_metadata(&path)
        }
        found => found,
    };
    match metadata {
        Ok(metadata) if metadata.is_file() => metadata.modified().map_err(io_error(&path)),
        Ok(_) => Err(Error::Unsafe(format!("{STORE}/{SET_UP}"))),
        Err(source) => Err(io_error(&path)(source)),
    }
}

/// Whether a conflict copy was made after `since`: Syncthing's rename to
/// make it sets its change time, and its name carries the time too, which
/// stays old however an old copy is touched later.
fn made_since(metadata: &fs::Metadata, conflict: &ConflictName<'_>, since: SystemTime) -> bool {
    let changed = u64::try_from(metadata.ctime())
        .ok()
        .and_then(|seconds| {
            let nanos = u32::try_from(metadata.ctime_nsec()).ok()?;
            UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
        })
        .unwrap_or(UNIX_EPOCH);
    let since_seconds = since.duration_since(UNIX_EPOCH).map_or(0, |since| {
        i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
    });
    let offset = i64::try_from(MAX_UTC_OFFSET.as_secs()).expect("small");
    changed >= since && conflict.at_as_utc() + offset >= since_seconds
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
        // A filesystem without it, such as NFS: a hard link also refuses
        // to replace a file.
        Err(rustix::io::Errno::INVAL) => match fs::hard_link(from, to) {
            Ok(()) => fs::remove_file(from).map_err(io_error(from)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Err(Error::Exists(to.to_owned()))
            }
            Err(source) => Err(io_error(from)(source)),
        },
        Err(errno) => Err(io_error(from)(errno.into())),
    }
}

/// What moving conflict copies into the store did.
#[derive(Debug, Default)]
pub struct Collected {
    pub found: Vec<DataConflict>,
    /// What couldn't be moved or looked at; those copies stay where they
    /// are, ignored.
    pub failed: Vec<Error>,
}

/// Moves the conflict copies directly in the directory `dir` of the Replica
/// at `root` into the store.
pub fn collect(root: &Path, dir: &str) -> Result<Collected, Error> {
    let _one = one_at_a_time();
    check_mounted(root)?;
    let since = set_up_at(root)?;
    let mut collected = Collected::default();
    if let Err(error) = collect_into(root, since, dir, &mut collected) {
        collected.failed.push(error);
    }
    Ok(collected)
}

/// Whether a directory at the root is internal, holding no conflict
/// copies.
fn internal(name: &str) -> bool {
    [STORE, MARKER, VERSIONS].contains(&name)
}

fn collect_into(
    root: &Path,
    since: SystemTime,
    dir: &str,
    collected: &mut Collected,
) -> Result<(), Error> {
    let parts = components(dir)?;
    if parts.first().is_some_and(|first| internal(first)) {
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
        let Some(conflict) = parse(&name) else {
            continue;
        };
        let metadata = entry.metadata().map_err(io_error(&path))?;
        if !metadata.is_file() || !made_since(&metadata, &conflict, since) {
            continue;
        }
        match displace(root, &parts, &name) {
            Ok(conflict) => collected.found.push(conflict),
            Err(error) => collected.failed.push(error),
        }
    }
    Ok(())
}

/// Moves the conflict copy `name` in the directory `parts` into the store.
fn displace(root: &Path, parts: &[&str], name: &str) -> Result<DataConflict, Error> {
    let conflict = parse(name).expect("a conflict copy's name");
    let mut inner = parts.to_vec();
    inner.push(name);
    let dir = stored(root, &inner, true)?.expect("created");
    if let Some((existing, _)) = unresolved(&dir)? {
        return Err(Error::Exists(existing));
    }
    let moved = seconds_now();
    let source = root.join(join(parts, name));
    move_new(&source, &dir.join(format!("{UNRESOLVED}{moved}")))?;
    Ok(data_conflict(&inner, &conflict, moved))
}

fn join(parts: &[&str], name: &str) -> String {
    let mut path = parts.join("/");
    if !path.is_empty() {
        path.push('/');
    }
    path.push_str(name);
    path
}

/// Moves every conflict copy in the Replica at `root` into the store.
pub fn sweep(root: &Path) -> Result<Collected, Error> {
    let _one = one_at_a_time();
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
                let skipped = dir.is_empty() && internal(&name);
                if !skipped && entry.file_type().map_err(io_error(&path))?.is_dir() {
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

/// Visits each conflict's directory in the store of the Replica at `root`,
/// with its path there and its copy's name.
fn each_conflict(
    root: &Path,
    mut visit: impl FnMut(&Path, &[&str], &ConflictName<'_>) -> Result<(), Error>,
) -> Result<(), Error> {
    let Some(store) = stored(root, &[], false)? else {
        return Ok(());
    };
    let mut dirs = vec![(store, Vec::<String>::new())];
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
    let _one = one_at_a_time();
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
/// Replica at `root`'s store for longer than `retention` at `now`, and
/// returns how many.
pub fn expire(root: &Path, retention: Duration, now: Timestamp) -> Result<usize, Error> {
    let _one = one_at_a_time();
    check_mounted(root)?;
    let now = now.as_millis() / 1000;
    let mut removed = 0;
    each_conflict(root, |dir, _, _| {
        for entry in fs::read_dir(dir).map_err(io_error(dir))? {
            let entry = entry.map_err(io_error(dir))?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(moved) = [UNRESOLVED, SWAPPING, RESOLVED]
                .into_iter()
                .find_map(|prefix| leaf_time(&name, prefix))
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
    let dir = stored(root, &parts, false)?.ok_or_else(not_found)?;
    let (loser, moved) = unresolved(&dir)?.ok_or_else(not_found)?;
    Ok((data_conflict(&parts, &conflict, moved), loser))
}

/// `loser`'s sibling with `prefix`, timed now.
fn sibling(loser: &Path, prefix: &str) -> PathBuf {
    loser.with_file_name(format!("{prefix}{}", seconds_now()))
}

/// Keeps the winning change of the Data Conflict `version`.
pub fn keep(root: &Path, version: &str) -> Result<DataConflict, Error> {
    let _one = one_at_a_time();
    let (conflict, loser) = find(root, version)?;
    move_new(&loser, &sibling(&loser, RESOLVED))?;
    Ok(conflict)
}

/// Makes the losing change of the Data Conflict `version` the file's
/// content again, keeping the content it replaces. The two are exchanged
/// in one rename, so the file is never missing, and a change made to it
/// meanwhile is what gets kept rather than lost. Once it returns, the
/// file has changed.
pub fn swap(root: &Path, version: &str) -> Result<DataConflict, Error> {
    let _one = one_at_a_time();
    let (conflict, loser) = find(root, version)?;
    let parts = components(&conflict.path)?;
    let (name, dirs) = parts.split_last().expect("a file's path has a name");
    let live_dir =
        directory(root, dirs, false)?.ok_or_else(|| Error::Unsafe(conflict.path.clone()))?;
    let live = live_dir.join(name);
    let live_exists = is_file(&live, &conflict.path)?;
    // Newer than the change it replaces, and changed enough for Syncthing
    // to look at its content again. Setting it needs only ownership.
    fs::File::open(&loser)
        .and_then(|file| file.set_modified(SystemTime::now()))
        .map_err(io_error(&loser))?;
    // Timed afresh, so that whichever content it holds when a swap is cut
    // short gets a whole Retention Window.
    let swapping = sibling(&loser, SWAPPING);
    move_new(&loser, &swapping)?;
    let installed = if live_exists {
        renameat_with(CWD, &swapping, CWD, &live, RenameFlags::EXCHANGE)
            .map_err(|errno| io_error(&live)(errno.into()))
    } else {
        move_new(&swapping, &live)
    };
    if let Err(error) = installed {
        let _ = move_new(&swapping, &loser);
        return Err(error);
    }
    if live_exists {
        // `swapping` now holds what was the file's content. Left as it is
        // if this fails, it still expires as a kept side does.
        let _ = move_new(&swapping, &sibling(&loser, RESOLVED));
    }
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
        set_up_moved(dir.path(), SystemTime::now() - Duration::from_secs(60));
        dir
    }

    fn set_up_moved(root: &Path, to: SystemTime) {
        fs::File::options()
            .write(true)
            .open(root.join(STORE).join(SET_UP))
            .unwrap()
            .set_modified(to)
            .unwrap();
    }

    fn write(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn read(root: &Path, path: &str) -> String {
        fs::read_to_string(root.join(path)).unwrap()
    }

    /// The one file in a conflict's directory, which has `prefix`.
    fn only_leaf(root: &Path, version: &str, prefix: &str) -> String {
        let dir = format!("{STORE}/{CONFLICTS}/{version}");
        let leaves: Vec<String> = fs::read_dir(root.join(&dir))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(leaves.len(), 1, "{leaves:?}");
        assert!(leaves[0].starts_with(prefix), "{leaves:?}");
        format!("{dir}/{}", leaves[0])
    }

    /// A copy named as made now.
    fn copy() -> String {
        let now = Timestamp::now().to_string();
        let at: String = now[..19].chars().filter(|c| c.is_ascii_digit()).collect();
        format!("notes.sync-conflict-{}-{}-ABCDEFG.txt", &at[..8], &at[8..])
    }

    const OLD_COPY: &str = "notes.sync-conflict-20240101-120000-ABCDEFG.txt";

    #[test]
    fn conflict_copy_names_parse() {
        assert_eq!(
            parse(OLD_COPY),
            Some(ConflictName {
                stem: "notes",
                at: "20240101-120000",
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
            // Not ASCII where the time or the ID go.
            "notes.sync-conflict-1234567é123456-ABCDEFG.txt",
            "a.sync-conflict-20261121-073910-ABCDEFé.txt",
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
    fn names_times_read_as_utc() {
        let at = |name: &str| parse(name).unwrap().at_as_utc();
        assert_eq!(at("a.sync-conflict-19700101-000000-ABCDEFG"), 0);
        assert_eq!(at("a.sync-conflict-20000229-000000-ABCDEFG"), 951_782_400);
        assert_eq!(at("a.sync-conflict-20261121-075959-ABCDEFG"), 1_795_247_999);
    }

    #[test]
    fn conflict_copies_move_into_the_store() {
        let root = replica();
        let root = root.path();
        let copy = copy();
        write(root, &format!("docs/{copy}"), "lost");
        write(root, "docs/notes.txt", "won");
        let found = collect(root, "docs").unwrap().found;
        let [conflict] = &found[..] else {
            panic!("{found:?}");
        };
        assert_eq!(conflict.path, "docs/notes.txt");
        assert_eq!(conflict.winner, "ABCDEFG");
        assert_eq!(conflict.version, format!("docs/{copy}"));
        assert!(!root.join("docs").join(&copy).exists());
        let loser = only_leaf(root, &conflict.version, UNRESOLVED);
        assert_eq!(read(root, &loser), "lost");
        assert_eq!(list(root).unwrap(), found);
    }

    #[test]
    fn copies_older_than_the_replica_stay_where_they_are() {
        let root = replica();
        let root = root.path();
        let copy = copy();
        write(root, &copy, "the user's");
        set_up_moved(root, SystemTime::now() + Duration::from_secs(60));
        let swept = sweep(root).unwrap();
        assert!(swept.found.is_empty() && swept.failed.is_empty());
        assert_eq!(read(root, &copy), "the user's");
        // One whose name is old stays even once touched.
        let root = replica();
        let root = root.path();
        write(root, OLD_COPY, "edited since");
        let swept = sweep(root).unwrap();
        assert!(swept.found.is_empty() && swept.failed.is_empty());
        assert_eq!(read(root, OLD_COPY), "edited since");
    }

    #[test]
    fn setting_up_again_moves_the_cutoff() {
        let root = replica();
        let root = root.path();
        let copy = copy();
        write(root, &copy, "made while it wasn't heighliner's");
        std::thread::sleep(Duration::from_millis(20));
        mark_set_up(root).unwrap();
        assert!(sweep(root).unwrap().found.is_empty());
    }

    #[test]
    fn a_long_name_still_fits() {
        let root = replica();
        let root = root.path();
        let copy = copy();
        // The longest name a copy can have on most filesystems.
        let long = format!("{}{}", "x".repeat(255 - copy.len()), copy);
        write(root, &long, "lost");
        let collected = collect(root, "").unwrap();
        assert!(collected.failed.is_empty(), "{:?}", collected.failed);
        assert_eq!(list(root).unwrap().len(), 1);
    }

    #[test]
    fn sweeps_find_nested_copies_without_following_symlinks() {
        let root = replica();
        let root = root.path();
        let copy = copy();
        write(root, &copy, "top");
        write(root, &format!("a/b/{copy}"), "nested");
        let outside = tempfile::tempdir().unwrap();
        write(outside.path(), &copy, "outside");
        std::os::unix::fs::symlink(outside.path(), root.join("link")).unwrap();
        let swept = sweep(root).unwrap();
        assert!(swept.failed.is_empty(), "{:?}", swept.failed);
        let mut found: Vec<String> = swept.found.into_iter().map(|c| c.path).collect();
        found.sort();
        assert_eq!(found, ["a/b/notes.txt", "notes.txt"]);
        assert!(outside.path().join(&copy).exists());
        let through_link = collect(root, "link").unwrap();
        assert!(matches!(through_link.failed[..], [Error::Unsafe(_)]));
    }

    #[test]
    fn copies_never_move_through_a_symlinked_store() {
        let root = replica();
        let root = root.path();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join(STORE).join(CONFLICTS)).unwrap();
        let copy = copy();
        write(root, &copy, "lost");
        let collected = collect(root, "").unwrap();
        assert!(matches!(collected.failed[..], [Error::Unsafe(_)]));
        assert_eq!(read(root, &copy), "lost");
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
    }

    #[test]
    fn keeping_leaves_the_loser_beside_it() {
        let root = replica();
        let root = root.path();
        write(root, &copy(), "lost");
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
        write(root, &format!("docs/{}", copy()), "lost");
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
    fn swapping_works_on_a_read_only_loser() {
        use std::os::unix::fs::PermissionsExt;
        let root = replica();
        let root = root.path();
        let copy = copy();
        write(root, &copy, "lost");
        fs::set_permissions(root.join(&copy), fs::Permissions::from_mode(0o444)).unwrap();
        write(root, "notes.txt", "won");
        let version = collect(root, "").unwrap().found.remove(0).version;
        swap(root, &version).unwrap();
        assert_eq!(read(root, "notes.txt"), "lost");
    }

    #[test]
    fn swapping_restores_a_deleted_file() {
        let root = replica();
        let root = root.path();
        write(root, &copy(), "lost");
        let version = collect(root, "").unwrap().found.remove(0).version;
        swap(root, &version).unwrap();
        assert_eq!(read(root, "notes.txt"), "lost");
        assert!(list(root).unwrap().is_empty());
    }

    #[test]
    fn sides_expire_from_when_they_were_moved() {
        let root = replica();
        let root = root.path();
        write(root, &copy(), "lost");
        write(root, "notes.txt", "won");
        let conflict = collect(root, "").unwrap().found.remove(0);
        let window = Duration::from_secs(30 * 24 * 60 * 60);
        let moved = conflict.moved_at;
        assert_eq!(
            expire(root, window, moved.saturating_add(window)).unwrap(),
            0
        );
        assert_eq!(list(root).unwrap().len(), 1);
        let later = moved.saturating_add(window + Duration::from_secs(1));
        assert_eq!(expire(root, window, later).unwrap(), 1);
        assert!(list(root).unwrap().is_empty());
        assert!(
            !root
                .join(STORE)
                .join(CONFLICTS)
                .join(&conflict.version)
                .exists()
        );
        // Nothing else in the Replica is expired.
        write(root, ".stversions/notes~20200101-000000.txt", "old");
        write(root, "docs/kept-1", "the user's");
        assert_eq!(expire(root, window, later).unwrap(), 0);
        assert!(root.join(".stversions/notes~20200101-000000.txt").exists());
        assert!(root.join("docs/kept-1").exists());
    }

    #[test]
    fn nothing_is_overwritten() {
        let root = replica();
        let root = root.path();
        let copy = copy();
        write(root, &copy, "lost");
        write(root, "notes.txt", "won");
        let version = collect(root, "").unwrap().found.remove(0).version;
        // An unresolved conflict with the same copy's name.
        write(root, &copy, "again");
        let collected = collect(root, "").unwrap();
        assert!(matches!(collected.failed[..], [Error::Exists(_)]));
        assert_eq!(read(root, &copy), "again");
        assert_eq!(list(root).unwrap()[0].version, version);
        assert_eq!(read(root, "notes.txt"), "won");
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
        let copy = copy();
        write(unmounted.path(), &copy, "lost");
        assert!(matches!(sweep(unmounted.path()), Err(Error::NotMounted)));
        assert!(unmounted.path().join(&copy).exists());
    }
}
