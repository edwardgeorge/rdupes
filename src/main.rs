use blake2::Blake2b;
use clap::{value_t, App, Arg, OsValues};
use digest::Digest;
use generic_array::{ArrayLength, GenericArray};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
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

fn find_duplicates<'a, D>(
    paths: &'a [FileInfo],
    hash_count: &mut usize,
) -> Result<Vec<Vec<&'a FileInfo>>, Error>
where
    D: Digest + Write,
{
    let mut matches: HashMap<_, Vec<&FileInfo>> = HashMap::new();
    let mut hashes: Vec<_> = paths
        .par_iter()
        .map(|i| hash_path::<D, _>(i).map(|h| (h, i)))
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

fn run(dirs: OsValues, options: &Options) -> Result<(), Error> {
    let num_hashes = Arc::new(AtomicUsize::new(0));
    let num_duplicates = Arc::new(AtomicUsize::new(0));
    let num_groups = Arc::new(AtomicUsize::new(0));
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
        let x = find_duplicates::<Blake2b>(&paths, &mut hash_count);
        num_hashes.fetch_add(hash_count, Ordering::Relaxed);
        match x {
            Err(e) => {
                eprintln!("error: {}", e);
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
        "{} total candidate files hashed, {} duplicates over {} groups. {} wasted bytes.",
        num_hashes.load(Ordering::SeqCst),
        num_duplicates.load(Ordering::SeqCst),
        num_groups.load(Ordering::SeqCst),
        total_sz.load(Ordering::SeqCst),
    );
    println!("{}\n{}", summary1, summary2);
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
                .help("maximum depth to recurse (0 is no recursion). implies -r."),
        )
        .arg(
            Arg::with_name("sort-opts")
                .long("sort-by")
                .takes_value(true)
                .help("properties to sort by, comma-separated. depth,mtime,path"),
        )
        .arg(
            Arg::with_name("prefer-within")
                .long("prefer-within")
                .takes_value(true)
                .help("prefer files within this path"),
        )
        .arg(Arg::with_name("directory").required(true).multiple(true))
        .get_matches();
    let dirs = matches.values_of_os("directory").unwrap();
    let recurse = matches.is_present("recursive") || matches.is_present("max-depth");
    let follow_symlinks = matches.is_present("follow");
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
    let prefer_location = if matches.is_present("prefer-within") {
        let p = value_t!(matches, "prefer-within", PathBuf).unwrap_or_else(|e| e.exit());
        Some(p.canonicalize().expect("could not canonicalize path"))
    } else {
        None
    };
    let sort_by = if matches.is_present("sort-opts") {
        value_t!(matches, "sort-opts", SortKeys).unwrap_or_else(|e| e.exit())
    } else {
        SortKeys::default()
    };
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
