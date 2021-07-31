use crate::types::{Error, FileInfo};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::Hash;
use std::path::{Path, PathBuf};

fn filename_sort_key(
    inp: &Path,
) -> (
    Option<&Path>,
    Option<&std::ffi::OsStr>,
    Option<&std::ffi::OsStr>,
) {
    (inp.parent(), inp.file_stem(), inp.extension())
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum SortBy {
    Depth,
    ModificationTime,
    PathParts,
}
use SortBy::*;

impl SortBy {
    #[inline]
    fn cmp_for_fileinfos(&self, left: &FileInfo, right: &FileInfo) -> Ordering {
        match self {
            Depth => left.depth.cmp(&right.depth),
            ModificationTime => left.mtime.cmp(&right.mtime),
            PathParts => filename_sort_key(&left.path).cmp(&filename_sort_key(&right.path)),
        }
    }
}

impl std::str::FromStr for SortBy {
    type Err = Error;
    fn from_str(s: &str) -> Result<SortBy, Error> {
        Ok(match s {
            "depth" => Depth,
            "mtime" => ModificationTime,
            "path" => PathParts,
            _ => return Err(Error::InvalidSortKey(s.to_string())),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SortKeys {
    keys: Vec<SortBy>,
}

impl SortKeys {
    pub fn new(mut keys: Vec<SortBy>) -> Option<Self> {
        let mut def = SortKeys::default().keys;
        let uniq: HashSet<_> = keys.iter().cloned().collect();
        if uniq.len() < keys.len() {
            // duplicated keys
            return None;
        }
        if keys.len() < def.len() {
            keys.extend(def.drain(..).filter(|k| !uniq.contains(k)));
        }
        Some(SortKeys { keys })
    }
}

impl std::default::Default for SortKeys {
    fn default() -> Self {
        SortKeys {
            keys: vec![ModificationTime, PathParts, Depth],
        }
    }
}

impl std::str::FromStr for SortKeys {
    type Err = Error;
    fn from_str(s: &str) -> Result<SortKeys, Error> {
        let keys: Vec<_> = s
            .split(',')
            .map(SortBy::from_str)
            .collect::<Result<_, _>>()?;
        SortKeys::new(keys).ok_or(Error::DuplicateSortKeys)
    }
}

impl SortKeys {
    #[inline]
    pub fn cmp_for_fileinfos(&self, left: &FileInfo, right: &FileInfo) -> Ordering {
        for key in &self.keys {
            let r = key.cmp_for_fileinfos(left, right);
            if !r.is_eq() {
                return r;
            }
        }
        Ordering::Equal
    }
}

#[derive(Debug, Clone)]
pub struct SortOptions {
    pub prefer_location: Option<PathBuf>,
    pub sort_by: SortKeys,
}

fn common_path(left: &Path, right: &Path) -> PathBuf {
    left.components()
        .zip(right.components())
        .take_while(|(a, b)| a == b)
        .map(|a| a.0)
        .collect()
}

fn is_within_dir(target: &Path, is_within: &Path) -> bool {
    common_path(target, is_within) == is_within
}

impl SortOptions {
    #[inline]
    pub fn cmp_for_fileinfos(&self, left: &FileInfo, right: &FileInfo) -> Ordering {
        if let Some(path) = &self.prefer_location {
            let l = is_within_dir(&left.path, path);
            let r = is_within_dir(&right.path, path);
            match (l, r) {
                (false, true) => return Ordering::Greater,
                (true, false) => return Ordering::Less,
                _ => {}
            }
        }
        self.sort_by.cmp_for_fileinfos(left, right)
    }
}

#[cfg(test)]
mod tests {
    use super::is_within_dir;
    use std::path::PathBuf;

    #[test]
    fn test_is_within_dir() {
        let trailing = PathBuf::from("/Users/myuser/Documents/");
        let notrail = PathBuf::from("/Users/myuser/Documents");
        let file = PathBuf::from("/Users/myuser/Documents/PDFs/Draft.pdf");
        assert!(is_within_dir(&file, &trailing));
        assert!(is_within_dir(&file, &notrail));
    }
}
