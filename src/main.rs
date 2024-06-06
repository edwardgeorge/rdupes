use blake3::{Hash, Hasher};
use clap::{arg, command, value_parser, Arg};
use rayon::prelude::*;
use std::collections::HashMap;

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::vec::Vec;

mod sorting;
mod types;

use sorting::{SortKeys, SortOptions};
use types::{Error, FileInfo};

#[derive(Debug, Clone)]
struct Options {
    recurse: bool,
    follow_symlinks: bool,
    min_size: u64,
    max_depth: Option<u64>,
    sort_options: SortOptions,
}

fn find_same_sized_files<I>(
    paths: I,
    table: &mut HashMap<u64, Vec<FileInfo>>,
    options: &Options,
) -> Result<(usize, usize, usize), Error>
where
    I: Iterator<Item = Result<(usize, PathBuf), Error>>,
{
    let mut files = 0;
    let mut seen = 0;
    let mut skipped = 0;
    for item in paths {
        let (depth, path) = item?;
        seen += 1;
        if path.is_file() {
            files += 1;
            let metadata = path.metadata()?;
            let size = metadata.len();
            if size >= options.min_size {
                let f = FileInfo {
                    depth,
                    mtime: metadata.modified().ok(),
                    path,
                };
                match table.get_mut(&size) {
                    None => {
                        //table.insert(size, vec![path.to_path_buf()]);
                        table.insert(size, vec![f]);
                    }
                    Some(x) => {
                        x.push(f)
                        //x.push(path.to_path_buf());
                        //table.insert(size, x);
                    }
                };
            } else {
                skipped += 1;
            }
        }
    }
    Ok((seen, files, skipped))
}

fn hash_path(path: &Path) -> io::Result<Hash> {
    let mut hasher = Hasher::new();
    hasher.update_mmap_rayon(path)?;
    Ok(hasher.finalize())
}

fn find_duplicates<'a>(
    paths: &'a [FileInfo],
    hash_count: &mut usize,
) -> Result<Vec<Vec<&'a FileInfo>>, Error> {
    let mut matches: HashMap<_, Vec<&FileInfo>> = HashMap::new();
    let mut hashes: Vec<_> = paths
        .par_iter()
        .map(|i| hash_path(i).map(|h| (*h.as_bytes(), i)))
        .collect();
    *hash_count = hashes.len();
    for i in hashes.drain(..) {
        let (h, p) = i?;
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

fn run<I, J>(dirs: I, options: &Options) -> Result<(), Error>
where
    I: IntoIterator<Item = J>,
    J: AsRef<Path>,
{
    let num_hashes = Arc::new(AtomicUsize::new(0));
    let num_duplicates = Arc::new(AtomicUsize::new(0));
    let num_groups = Arc::new(AtomicUsize::new(0));
    let num_errors = Arc::new(AtomicUsize::new(0));
    let total_sz = Arc::new(AtomicU64::new(0));
    let depth = if options.recurse {
        options.max_depth
    } else {
        Some(0)
    };
    let mut table = HashMap::new();
    let mut seen_counter = 0;
    let mut files_counter = 0;
    let mut skipped_counter = 0;
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
        let (seen, files, skipped) = find_same_sized_files(i, &mut table, options)?;
        seen_counter += seen;
        files_counter += files;
        skipped_counter += skipped;
    }
    table.par_drain().for_each(|(sz, paths)| {
        if paths.len() < 2 {
            return;
        }
        let mut hash_count = 0;
        let x = find_duplicates(&paths, &mut hash_count);
        num_hashes.fetch_add(hash_count, Ordering::Relaxed);
        match x {
            Err(e) => {
                eprintln!("error attempting to hash file from {}B group: {}", sz, e);
                num_errors.fetch_add(1, Ordering::Relaxed);
            }
            Ok(mut paths) => {
                num_groups.fetch_add(paths.len(), Ordering::Relaxed);
                let stdout = std::io::stdout();
                for grp in paths.iter_mut() {
                    let grplen = grp.len();
                    num_duplicates.fetch_add(grplen, Ordering::Relaxed);
                    total_sz.fetch_add(sz * (grplen as u64 - 1), Ordering::Relaxed);
                    grp.sort_unstable_by(|l, r| options.sort_options.cmp_for_fileinfos(l, r));
                    let mut out = stdout.lock();
                    let _ = writeln!(out, "\u{250C} {:?} bytes", sz);
                    for (k, p) in grp.iter().enumerate() {
                        if k < grplen - 1 {
                            let _ = writeln!(out, "\u{251C} {}", p.display());
                        } else {
                            let _ = writeln!(out, "\u{2514} {}\n", p.display());
                        }
                    }
                }
            }
        }
    });
    let summary1 = format!(
        "{} regular files seen (of {} files total), {} skipped by min-size ({}B).",
        files_counter, seen_counter, skipped_counter, options.min_size
    );
    let summary2 = format!(
        "{} total candidate files hashed, {} errors. {} duplicates over {} groups. {} wasted bytes.",
        num_hashes.load(Ordering::SeqCst),
        num_errors.load(Ordering::SeqCst),
        num_duplicates.load(Ordering::SeqCst),
        num_groups.load(Ordering::SeqCst),
        total_sz.load(Ordering::SeqCst),
    );
    println!("{}\n{}", summary1, summary2);
    Ok(())
}

fn main() {
    let matches = command!()
        .arg(
            arg!(recursive: -r "recurse into directories"),
        )
        .arg(
            arg!(follow: -f --follow "follow symlinks"),
        )
        .arg(
            arg!(--"min-size" <BYTES> "minimum size of files (in bytes) to find duplicates for")
                .value_parser(value_parser!(u64)),
        )
        .arg(
            arg!(--"max-depth" <DEPTH> "maximum depth to recurse (0 is no recursion). implies -r.")
                .value_parser(value_parser!(u64)),
        )
        .arg(
            arg!(--"sort-by" <PROPS> "properties to sort by, comma-separated. depth,mtime,path")
                .value_parser(SortKeys::from_str),
        )
        .arg(
            arg!(--"prefer-within" <PATH> "prefer files within this path")
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("directory")
                .required(true)
                .num_args(1..)
                .value_parser(value_parser!(PathBuf)),
        )
        .get_matches();
    let dirs: Vec<&PathBuf> = matches.get_many("directory").unwrap().collect();
    let recurse = matches.get_flag("recursive") || matches.contains_id("max-depth");
    let follow_symlinks = matches.get_flag("follow");
    let min_size: u64 = matches.get_one("min-size").copied().unwrap_or(1);
    let max_depth = matches.get_one::<u64>("max-depth").copied();
    let prefer_location = matches.get_one::<PathBuf>("prefer-within").map(|p| p.canonicalize().expect("could not canonicalize path"));
    let sort_by = matches
        .get_one::<SortKeys>("sort-opts")
        .cloned()
        .unwrap_or_else(SortKeys::default);
    let sort_options = SortOptions {
        prefer_location,
        sort_by,
    };
    let result = run(
        dirs,
        &Options {
            recurse,
            follow_symlinks,
            min_size,
            max_depth,
            sort_options,
        },
    );
    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
