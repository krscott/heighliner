//! Data Conflicts, kept among a Replica's Displaced Versions.
//!
//! When two Replicas change a file concurrently, Syncthing keeps the newer
//! change and renames the other file beside it, as
//! `<stem>.sync-conflict-<date>-<time>-<device><ext>`, where `<device>` is
//! the short ID of the device whose change won. Syncthing can't keep that
//! copy anywhere else, so every folder ignores [`IGNORE_PATTERN`], keeping
//! the copy on the Node it was made on, and heighliner moves it into
//! `.stversions` tagged as Syncthing's versioner tags a version,
//! `~<date>-<time>`, with the conflict's time. Syncthing then expires it
//! with the folder's other Displaced Versions.
//!
//! The Data Conflicts are the files in `.stversions` with a conflict copy's
//! name, so they need no other record. Keeping or swapping one leaves the
//! side not chosen in `.stversions` as an ordinary version of the file,
//! with the same tag, until it expires.
//!
//! Everything here works on the filesystem and blocks; paths are relative
//! to a Replica's root and use `/`.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

/// The ignore pattern that keeps conflict copies on the Node they were
/// made on.
pub const IGNORE_PATTERN: &str = "*.sync-conflict-*";

const CONFLICT: &str = ".sync-conflict-";

/// Where Syncthing keeps a folder's Displaced Versions, which it ignores.
const VERSIONS: &str = ".stversions";

/// Syncthing ignores files whose names start with this, which it uses for
/// its own temporary files.
const TEMPORARY: &str = ".syncthing.";

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
    /// The losing version's path within `.stversions`, which identifies
    /// the conflict.
    pub version: String,
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
    Exists(String),
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
            Self::Exists(path) => write!(f, "{path} already exists"),
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

fn parse(name: &str) -> Option<ConflictName<'_>> {
    let (stem, rest) = name.split_once(CONFLICT)?;
    let at = rest.get(..15)?;
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if !(digits(&at[..8]) && at.as_bytes()[8] == b'-' && digits(&at[9..])) {
        return None;
    }
    let rest = rest[15..].strip_prefix('-')?;
    // A device's short ID has no dot, so the extension is what follows one.
    let (winner, ext) = rest.split_at(rest.find('.').unwrap_or(rest.len()));
    if winner.is_empty() {
        return None;
    }
    Some(ConflictName {
        stem,
        at,
        winner,
        ext,
    })
}

/// A name's extension as Go's `filepath.Ext` gives it, which is where
/// Syncthing puts a version's tag: from its last dot.
fn extension(name: &str) -> &str {
    name.rfind('.').map_or("", |dot| &name[dot..])
}

/// `name` tagged as a version from `at`, as Syncthing's versioner names
/// versions.
fn tagged(name: &str, at: &str) -> String {
    let ext = extension(name);
    format!("{}~{at}{ext}", &name[..name.len() - ext.len()])
}

/// A version's name split into the name it is a version of and its tag.
fn untagged(name: &str) -> Option<(String, &str)> {
    let ext = extension(name);
    let (stem, tag) = name[..name.len() - ext.len()].rsplit_once('~')?;
    (!tag.is_empty() && !tag.contains('.')).then(|| (format!("{stem}{ext}"), tag))
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
/// directory rather than a symlink. `None` if it doesn't exist.
fn directory(root: &Path, parts: &[&str]) -> Result<Option<PathBuf>, Error> {
    let mut dir = root.to_owned();
    for part in parts {
        dir.push(part);
        match fs::symlink_metadata(&dir) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(Error::Unsafe(parts.join("/"))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(Error::Io { path: dir, source }),
        }
    }
    Ok(Some(dir))
}

fn check_mounted(root: &Path) -> Result<(), Error> {
    if root.join(".stfolder").is_dir() {
        Ok(())
    } else {
        Err(Error::NotMounted)
    }
}

fn is_file(path: &Path) -> Result<bool, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io_error(path)(source)),
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
    let mut collected = Collected::default();
    if let Err(error) = collect_into(root, dir, &mut collected) {
        collected.failed.push(error);
    }
    Ok(collected)
}

fn collect_into(root: &Path, dir: &str, collected: &mut Collected) -> Result<(), Error> {
    let parts = components(dir)?;
    if parts.first() == Some(&VERSIONS) {
        return Ok(());
    }
    let Some(path) = directory(root, &parts)? else {
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
    let mut versions = vec![VERSIONS];
    versions.extend_from_slice(parts);
    let mut target = root.join(versions.join("/"));
    fs::create_dir_all(&target).map_err(io_error(&target))?;
    let version = tagged(name, conflict.at);
    target.push(&version);
    let relative = join(&versions[1..], &version);
    if fs::symlink_metadata(&target).is_ok() {
        return Err(Error::Exists(format!("{VERSIONS}/{relative}")));
    }
    let source = root.join(join(parts, name));
    fs::rename(&source, &target).map_err(io_error(&source))?;
    Ok(DataConflict {
        path: join(parts, &conflict.original()),
        at: conflict.at.to_owned(),
        winner: conflict.winner.to_owned(),
        version: relative,
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
/// Versions. Symlinks aren't followed.
pub fn sweep(root: &Path) -> Result<Collected, Error> {
    check_mounted(root)?;
    let mut collected = Collected::default();
    let mut dirs = vec![String::new()];
    while let Some(dir) = dirs.pop() {
        let mut visit = || -> Result<(), Error> {
            collect_into(root, &dir, &mut collected)?;
            let path = root.join(&dir);
            for entry in fs::read_dir(&path).map_err(io_error(&path))? {
                let entry = entry.map_err(io_error(&path))?;
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                let internal = dir.is_empty() && (name == VERSIONS || name == ".stfolder");
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

/// The Data Conflicts kept in the Replica at `root`.
pub fn list(root: &Path) -> Result<Vec<DataConflict>, Error> {
    check_mounted(root)?;
    let mut conflicts = Vec::new();
    let Some(versions) = directory(root, &[VERSIONS])? else {
        return Ok(conflicts);
    };
    let mut dirs = vec![(versions, Vec::<String>::new())];
    while let Some((path, parts)) = dirs.pop() {
        for entry in fs::read_dir(&path).map_err(io_error(&path))? {
            let entry = entry.map_err(io_error(&path))?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let file_type = entry.file_type().map_err(io_error(&path))?;
            let parts_str: Vec<&str> = parts.iter().map(String::as_str).collect();
            if file_type.is_dir() {
                let mut inner = parts.clone();
                inner.push(name);
                dirs.push((entry.path(), inner));
            } else if file_type.is_file()
                && let Some(conflict) = conflict_of(&parts_str, &name)
            {
                conflicts.push(conflict);
            }
        }
    }
    conflicts.sort_by(|a, b| (&a.path, &a.at).cmp(&(&b.path, &b.at)));
    Ok(conflicts)
}

/// The Data Conflict whose losing version is `name` in the directory
/// `parts` of `.stversions`, if it is one.
fn conflict_of(parts: &[&str], name: &str) -> Option<DataConflict> {
    let (copy, tag) = untagged(name)?;
    let conflict = parse(&copy)?;
    (conflict.at == tag).then(|| DataConflict {
        path: join(parts, &conflict.original()),
        at: conflict.at.to_owned(),
        winner: conflict.winner.to_owned(),
        version: join(parts, name),
    })
}

/// The Data Conflict `version` in the Replica at `root`, and its path in
/// `.stversions`.
fn find(root: &Path, version: &str) -> Result<(DataConflict, PathBuf), Error> {
    check_mounted(root)?;
    let not_found = || Error::NotFound(version.to_owned());
    let parts = components(version)?;
    let (name, dirs) = parts.split_last().ok_or_else(not_found)?;
    let conflict = conflict_of(dirs, name).ok_or_else(not_found)?;
    let mut versions = vec![VERSIONS];
    versions.extend_from_slice(dirs);
    let dir = directory(root, &versions)?.ok_or_else(not_found)?;
    let path = dir.join(name);
    if !is_file(&path)? {
        return Err(not_found());
    }
    Ok((conflict, path))
}

/// Where the side of `conflict` not chosen is kept: an ordinary version of
/// the file, from the conflict's time.
fn displaced(root: &Path, conflict: &DataConflict) -> PathBuf {
    let (dir, name) = conflict
        .path
        .rsplit_once('/')
        .unwrap_or(("", conflict.path.as_str()));
    let mut path = root.join(VERSIONS);
    if !dir.is_empty() {
        path.push(dir);
    }
    path.join(tagged(name, &conflict.at))
}

/// Keeps the winning change of the Data Conflict `version`.
pub fn keep(root: &Path, version: &str) -> Result<DataConflict, Error> {
    let (conflict, path) = find(root, version)?;
    let target = displaced(root, &conflict);
    if fs::symlink_metadata(&target).is_ok() {
        return Err(Error::Exists(target.display().to_string()));
    }
    fs::rename(&path, &target).map_err(io_error(&path))?;
    Ok(conflict)
}

/// Makes the losing change of the Data Conflict `version` the file's
/// content again, keeping the content it replaces as a Displaced Version.
/// The file is never missing meanwhile, so Syncthing can't see it deleted.
pub fn swap(root: &Path, version: &str) -> Result<DataConflict, Error> {
    let (conflict, path) = find(root, version)?;
    let target = displaced(root, &conflict);
    if fs::symlink_metadata(&target).is_ok() {
        return Err(Error::Exists(target.display().to_string()));
    }
    let parts = components(&conflict.path)?;
    let (name, dirs) = parts.split_last().expect("a file's path has a name");
    let dir = directory(root, dirs)?.ok_or_else(|| Error::Unsafe(conflict.path.clone()))?;
    let live = dir.join(name);
    let live_exists = match fs::symlink_metadata(&live) {
        Ok(metadata) if metadata.is_file() => true,
        Ok(_) => return Err(Error::Unsafe(conflict.path.clone())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(source) => return Err(io_error(&live)(source)),
    };
    let staged = dir.join(format!("{TEMPORARY}{name}.heighliner"));
    if fs::symlink_metadata(&staged).is_ok() {
        return Err(Error::Exists(staged.display().to_string()));
    }
    fs::rename(&path, &staged).map_err(io_error(&path))?;
    let undo = |error: Error| {
        let _ = fs::rename(&staged, &path);
        error
    };
    if live_exists {
        fs::hard_link(&live, &target)
            .map_err(io_error(&live))
            .map_err(undo)?;
    }
    if let Err(source) = fs::rename(&staged, &live) {
        if live_exists {
            let _ = fs::remove_file(&target);
        }
        return Err(undo(io_error(&staged)(source)));
    }
    Ok(conflict)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join(".stfolder")).unwrap();
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
        assert_eq!(
            original("archive.tar.sync-conflict-20261121-073910-ABCDEFG.gz").as_deref(),
            Some("archive.tar.gz")
        );
        assert_eq!(
            original("Makefile.sync-conflict-20261121-073910-ABCDEFG").as_deref(),
            Some("Makefile")
        );
        assert_eq!(
            original(".sync-conflict-20261121-073910-ABCDEFG.bashrc").as_deref(),
            Some(".bashrc")
        );
        assert_eq!(parse("notes.txt"), None);
        assert_eq!(parse("notes.sync-conflict-2026-ABCDEFG.txt"), None);
    }

    #[test]
    fn versions_are_tagged_as_syncthing_tags_them() {
        // Syncthing's TestTaggedFilename cases.
        for (name, tag, version) in [
            ("bar.baz", "tag", "bar~tag.baz"),
            ("bar", "tag", "bar~tag"),
            (
                "~$ufheft2.docx",
                "20140612-200554",
                "~$ufheft2~20140612-200554.docx",
            ),
            (
                "alle~4.mgz",
                "20141106-094415",
                "alle~4~20141106-094415.mgz",
            ),
        ] {
            assert_eq!(tagged(name, tag), version);
            assert_eq!(untagged(version), Some((name.to_owned(), tag)));
        }
        // A conflict copy without an extension is tagged before the last
        // dot, as Syncthing's versioner reads it.
        let copy = "Makefile.sync-conflict-20261121-073910-ABCDEFG";
        let version = tagged(copy, "20261121-073910");
        assert_eq!(
            version,
            "Makefile~20261121-073910.sync-conflict-20261121-073910-ABCDEFG"
        );
        assert_eq!(
            untagged(&version),
            Some((copy.to_owned(), "20261121-073910"))
        );
        assert_eq!(
            untagged(&tagged(COPY, "20261121-073910")),
            Some((COPY.to_owned(), "20261121-073910"))
        );
        assert_eq!(untagged("foo.txt"), None);
    }

    #[test]
    fn conflict_copies_move_into_displaced_versions() {
        let root = replica();
        let root = root.path();
        write(root, &format!("docs/{COPY}"), "lost");
        write(root, "docs/notes.txt", "won");
        let found = collect(root, "docs").unwrap().found;
        let version = "docs/notes.sync-conflict-20261121-073910-ABCDEFG~20261121-073910.txt";
        assert_eq!(
            found,
            vec![DataConflict {
                path: "docs/notes.txt".into(),
                at: "20261121-073910".into(),
                winner: "ABCDEFG".into(),
                version: version.into(),
            }]
        );
        assert!(!root.join("docs").join(COPY).exists());
        assert_eq!(read(root, &format!(".stversions/{version}")), "lost");
        assert_eq!(list(root).unwrap(), found);
        // Ordinary versions aren't conflicts.
        write(root, ".stversions/docs/notes~20261120-000000.txt", "older");
        assert_eq!(list(root).unwrap(), found);
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
    fn keeping_leaves_the_loser_as_an_ordinary_version() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "lost");
        write(root, "notes.txt", "won");
        let version = collect(root, "").unwrap().found.remove(0).version;
        keep(root, &version).unwrap();
        assert!(list(root).unwrap().is_empty());
        assert_eq!(read(root, ".stversions/notes~20261121-073910.txt"), "lost");
        assert_eq!(read(root, "notes.txt"), "won");
        assert!(matches!(keep(root, &version), Err(Error::NotFound(_))));
    }

    #[test]
    fn swapping_restores_the_loser_and_keeps_the_winner() {
        let root = replica();
        let root = root.path();
        write(root, &format!("docs/{COPY}"), "lost");
        write(root, "docs/notes.txt", "won");
        let version = collect(root, "docs").unwrap().found.remove(0).version;
        swap(root, &version).unwrap();
        assert!(list(root).unwrap().is_empty());
        assert_eq!(read(root, "docs/notes.txt"), "lost");
        assert_eq!(
            read(root, ".stversions/docs/notes~20261121-073910.txt"),
            "won"
        );
        let leftovers: Vec<_> = fs::read_dir(root.join("docs")).unwrap().collect();
        assert_eq!(leftovers.len(), 1);
    }

    #[test]
    fn swapping_stops_rather_than_overwriting_a_version() {
        let root = replica();
        let root = root.path();
        write(root, COPY, "lost");
        write(root, "notes.txt", "won");
        let version = collect(root, "").unwrap().found.remove(0).version;
        write(root, ".stversions/notes~20261121-073910.txt", "another");
        assert!(matches!(swap(root, &version), Err(Error::Exists(_))));
        assert_eq!(read(root, "notes.txt"), "won");
        assert_eq!(
            read(root, ".stversions/notes~20261121-073910.txt"),
            "another"
        );
        assert_eq!(list(root).unwrap().len(), 1);
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
