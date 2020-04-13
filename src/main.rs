use blake2::Blake2b;
use clap::{value_t, App, Arg, OsValues};
use digest::Digest;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::vec::Vec;

struct Options {
    recurse: bool,
    min_size: u64,
    max_depth: i64,
}

impl Options {
    fn decr_depth(&self) -> Options {
        Options {
            recurse: self.recurse,
            min_size: self.min_size,
            max_depth: self.max_depth - 1,
        }
    }
}

fn find_same_sized_files(
    path: &Path,
    table: &mut HashMap<u64, Vec<PathBuf>>,
    options: &Options,
) -> io::Result<()> {
    if path.is_dir() && options.max_depth != 0 {
        for entry in read_dir(path)? {
            let entry = entry?;
            find_same_sized_files(&entry.path(), table, &options.decr_depth())?;
        }
    } else if path.is_file() {
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
    Ok(())
}

fn hash_path<D, F, A>(path: &Path, kont: F) -> io::Result<A>
where
    D: Digest + io::Write,
    F: FnOnce(D) -> A,
{
    let mut file = File::open(path)?;
    let mut hasher = D::new();
    io::copy(&mut file, &mut hasher)?;
    Ok(kont(hasher))
}

fn find_duplicates<'a, D>(paths: &'a [PathBuf]) -> io::Result<Vec<Vec<&'a PathBuf>>>
where
    D: Digest + io::Write,
{
    let mut matches: HashMap<_, Vec<&'a PathBuf>> = HashMap::new();
    let paths = paths;
    let x = paths
        .par_iter()
        .map(|i| hash_path::<D, _, _>(i, |h| h.result()).map(|j| (i, j)))
        .collect::<io::Result<Vec<(&PathBuf, _)>>>()?;
    for (i, h) in x.iter() {
        match matches.remove(h) {
            None => {
                matches.insert(h, vec![i]);
            }
            Some(mut x) => {
                x.push(i);
                matches.insert(h, x);
            }
        };
    }
    let r = matches
        .drain()
        .map(|x| x.1)
        .filter(|x| x.len() > 1)
        .collect();
    Ok(r)
}

fn run(dirs: OsValues, options: &Options) -> io::Result<()> {
    let first = Mutex::new(true);
    let mut table = HashMap::new();
    for dir in dirs {
        find_same_sized_files(Path::new(dir), &mut table, options)?;
    }
    table
        .par_iter()
        .filter(|(_, x)| x.len() > 1)
        .map(|(sz, paths)| find_duplicates::<Blake2b>(&paths).map(|d| (sz, d)))
        .for_each(|x| match x {
            Err(e) => {
                eprintln!("error: {}", e);
            }
            Ok((sz, paths)) => {
                for grp in paths.iter() {
                    let grplen = grp.len();
                    let mut f = first.lock().unwrap();
                    if *f {
                        *f = false;
                    } else {
                        println!("");
                    }
                    println!("\u{250C} {:?} bytes", sz);
                    for (k, p) in grp.iter().enumerate() {
                        if k < grplen - 1 {
                            println!("\u{251C} {}", p.display());
                        } else {
                            println!("\u{2514} {}", p.display());
                        }
                    }
                }
            }
        });
    Ok(())
}

fn main() -> io::Result<()> {
    let matches = App::new("rdupes")
        .arg(Arg::with_name("recursive").short("r").takes_value(false))
        .arg(
            Arg::with_name("min-size")
                .long("min-size")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max-depth")
                .long("max-depth")
                .takes_value(true),
        )
        .arg(Arg::with_name("directory").required(true).multiple(true))
        .get_matches();
    let dirs = matches.values_of_os("directory").unwrap();
    let rec = matches.occurrences_of("recursive") > 0;
    let min_size = if matches.is_present("min-size") {
        value_t!(matches.value_of("min-size"), u64).unwrap_or_else(|e| e.exit())
    } else {
        1
    };
    let max_depth = if matches.is_present("max-depth") {
        value_t!(matches.value_of("max-depth"), i64).unwrap_or_else(|e| e.exit())
    } else {
        -1
    };
    run(
        dirs,
        &Options {
            recurse: rec,
            min_size,
            max_depth,
        },
    )
}
