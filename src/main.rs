use blake2::Blake2b;
use clap::{value_t, App, Arg};
use digest::Digest;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io;
use std::path::{Path, PathBuf};
use std::vec::Vec;

fn find_same_sized_files(
    dir: &Path,
    table: &mut HashMap<u64, Vec<PathBuf>>,
    recurse: bool,
    min_size: u64,
    max_depth: i64,
) -> io::Result<()> {
    // let table = HashMap::new();
    for entry in read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        // println!("found: {:?}", path);
        let typ = entry.file_type()?;
        if typ.is_dir() {
            if recurse && max_depth != 0 {
                find_same_sized_files(&path, table, recurse, min_size, max_depth - 1)?;
            }
        } else if typ.is_file() {
            let metadata = entry.metadata()?;
            let size = metadata.len();
            if size >= min_size {
                match table.remove(&size) {
                    None => {
                        table.insert(size, vec![path]);
                    }
                    Some(mut x) => {
                        x.push(path);
                        table.insert(size, x);
                    }
                };
            }
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
    // let h = hasher.result();
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


fn run(dir: &Path, recurse: bool, min_size: u64, max_depth: i64) -> io::Result<()> {
    let mut table = HashMap::new();
    find_same_sized_files(dir, &mut table, recurse, min_size, max_depth)?;
    // println!("res: {:?}", results);
    for (i, (sz, paths)) in table.drain().filter(|x| x.1.len() > 1).enumerate() {
        let x = find_duplicates::<Blake2b>(&paths)?;
        for grp in x.iter() {
            let grplen = grp.len();
            if i > 0 {
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
        // println!("{:?}", paths);
    }
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
        .arg(Arg::with_name("directory").required(true))
        .get_matches();
    let dir = matches.value_of_os("directory").unwrap();
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
    run(Path::new(dir), rec, min_size, max_depth)
}
