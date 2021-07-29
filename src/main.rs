use blake2::Blake2b;
use clap::{value_t, App, Arg, OsValues};
use digest::Digest;
use generic_array::{ArrayLength, GenericArray};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::vec::Vec;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Cycle in links detected at: {0}")]
    RecursiveLinks(PathBuf),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
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

struct Options {
    recurse: bool,
    follow_symlinks: bool,
    min_size: u64,
    max_depth: Option<u64>,
}

fn filename_sort_key<'a>(
    inp: &&'a PathBuf,
) -> (
    Option<&'a Path>,
    Option<&'a std::ffi::OsStr>,
    Option<&'a std::ffi::OsStr>,
) {
    (inp.parent(), inp.file_stem(), inp.extension())
}

fn find_same_sized_files<I>(
    paths: I,
    table: &mut HashMap<u64, Vec<PathBuf>>,
    options: &Options,
) -> Result<(), Error>
where
    I: Iterator<Item = Result<(usize, PathBuf), Error>>,
{
    for item in paths {
        let (_depth, path) = item?;
        if path.is_file() {
            let metadata = path.metadata()?;
            let size = metadata.len();
            if size >= options.min_size {
                match table.remove(&size) {
                    None => {
                        table.insert(size, vec![path.to_path_buf()]);
                    }
                    Some(mut x) => {
                        x.push(path.to_path_buf());
                        table.insert(size, x);
                    }
                };
            }
        }
    }
    Ok(())
}

fn hash_path<D, N>(path: &Path) -> io::Result<GenericArray<u8, N>>
where
    D: Digest<OutputSize = N> + Write,
    N: ArrayLength<u8>,
{
    let mut file = File::open(path)?;
    let mut hasher = D::new();
    io::copy(&mut file, &mut hasher)?;
    //let x: Vec<u8> = Vec::from(&hasher.finalize()[..]);
    Ok(hasher.finalize())
}

fn find_duplicates<D>(paths: &[PathBuf]) -> Result<Vec<Vec<&PathBuf>>, Error>
where
    D: Digest + Write,
{
    let mut matches: HashMap<_, Vec<&PathBuf>> = HashMap::new();
    let mut hashes: Vec<_> = paths
        .par_iter()
        .map(|i| hash_path::<D, _>(i).map(|h| (h, i)))
        .collect::<Result<_, _>>()?;
    for (h, p) in hashes.drain(..) {
        if let Some(existing) = matches.get_mut(&h) {
            existing.push(p);
        } else {
            matches.insert(h, vec![p]);
        }
    }
    let r = matches
        .drain()
        .map(|x| x.1)
        .filter(|x| x.len() > 1)
        .collect();
    Ok(r)
}

fn run(dirs: OsValues, options: &Options) -> Result<(), Error> {
    let first = Arc::new(AtomicBool::new(true));
    let depth = if options.recurse {
        options.max_depth
    } else {
        Some(0)
    };
    let mut table = HashMap::new();
    for dir in dirs {
        let mut iter = walkdir::WalkDir::new(dir);
        if let Some(d) = depth {
            iter = iter.max_depth(d as usize + 1);
        }
        if options.follow_symlinks {
            iter = iter.follow_links(true);
        }
        let i = iter
            .into_iter()
            .map(|d| d.map(|e| (e.depth(), e.into_path())).map_err(Error::from));
        find_same_sized_files(i, &mut table, options)?;
    }
    let mut keys: Vec<_> = table.keys().collect();
    keys.sort();
    keys.par_iter()
        .map(|sz| (sz, table.get(sz).unwrap()))
        .filter(|(_, x)| x.len() > 1)
        .map(|(sz, paths)| find_duplicates::<Blake2b>(&paths).map(|d| (sz, d)))
        .for_each(|x| match x {
            Err(e) => {
                eprintln!("error: {}", e);
            }
            Ok((sz, mut paths)) => {
                let stdout = std::io::stdout();
                for grp in paths.iter_mut() {
                    grp.sort_by_key(filename_sort_key);
                    let grplen = grp.len();
                    let mut out = stdout.lock();
                    if first.load(Ordering::SeqCst) {
                        first.store(false, Ordering::SeqCst);
                    } else {
                        let _ = writeln!(out);
                    }
                    let _ = writeln!(out, "\u{250C} {:?} bytes", sz);
                    for (k, p) in grp.iter().enumerate() {
                        if k < grplen - 1 {
                            let _ = writeln!(out, "\u{251C} {}", p.display());
                        } else {
                            let _ = writeln!(out, "\u{2514} {}", p.display());
                        }
                    }
                }
            }
        });
    Ok(())
}

fn main() {
    let matches = App::new("rdupes")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name("recursive")
                .short("r")
                .takes_value(false)
                .help("recurse into directories"),
        )
        .arg(
            Arg::with_name("follow")
                .short("f")
                .takes_value(false)
                .help("follow symlinks"),
        )
        .arg(
            Arg::with_name("min-size")
                .long("min-size")
                .takes_value(true)
                .help("minimum size of files to find duplicates for"),
        )
        .arg(
            Arg::with_name("max-depth")
                .long("max-depth")
                .takes_value(true)
                .help("maximum depth to recurse (0 is no recursion). requires -r flag.")
                .requires("recursive"),
        )
        .arg(Arg::with_name("directory").required(true).multiple(true))
        .get_matches();
    let dirs = matches.values_of_os("directory").unwrap();
    let recurse = matches.occurrences_of("recursive") > 0;
    let follow_symlinks = matches.occurrences_of("follow") > 0;
    let min_size = if matches.is_present("min-size") {
        value_t!(matches.value_of("min-size"), u64).unwrap_or_else(|e| e.exit())
    } else {
        1
    };
    let max_depth = if matches.is_present("max-depth") {
        Some(value_t!(matches.value_of("max-depth"), u64).unwrap_or_else(|e| e.exit()))
    } else {
        None
    };
    let result = run(
        dirs,
        &Options {
            recurse,
            follow_symlinks,
            min_size,
            max_depth,
        },
    );
    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
