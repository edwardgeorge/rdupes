use std::fmt;
use std::path::{Display, Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, PartialEq, Eq)]
pub struct FileInfo {
    pub depth: usize,
    pub mtime: Option<SystemTime>,
    pub path: PathBuf,
}

impl FileInfo {
    pub fn display(&self) -> Display<'_> {
        self.path.display()
    }
}

impl std::ops::Deref for FileInfo {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}

impl fmt::Display for FileInfo {
    fn fmt<'a>(&self, f: &mut fmt::Formatter<'a>) -> Result<(), fmt::Error> {
        write!(f, "{}", &self.path.display())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Cycle in links detected at: {0}")]
    RecursiveLinks(PathBuf),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("Invalid sort key: {0}")]
    InvalidSortKey(String),
    #[error("Duplicate sort keys provided")]
    DuplicateSortKeys,
}

impl From<walkdir::Error> for Error {
    fn from(err: walkdir::Error) -> Self {
        if let Some(path) = err.loop_ancestor() {
            Error::RecursiveLinks(path.to_owned())
        } else if let Some(error) = err.into_io_error() {
            Error::IOError(error)
        } else {
            panic!("walkdir return unknown error")
        }
    }
}
