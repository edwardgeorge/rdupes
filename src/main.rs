use blake2::Blake2b;
use clap::{value_t, App, Arg};
use digest::Digest;
use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io;
use std::iter::Map;
use std::path::{Path, PathBuf};
use std::slice::Iter;
use std::vec::Vec;

fn foo(
    dir: &Path,
    table: HashMap<u64, Vec<PathBuf>>,
    recurse: bool,
    min_size: u64,
) -> io::Result<HashMap<u64, Vec<PathBuf>>> {
    let mut table = table;
    // let table = HashMap::new();
    for entry in read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        // println!("found: {:?}", path);
        let typ = entry.file_type()?;
        if typ.is_dir() {
            if recurse {
                table = foo(&path, table, recurse, min_size)?;
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
    Ok(table)
}

fn bar<D: Digest + io::Write>(paths: Vec<PathBuf>) -> io::Result<Vec<Vec<PathBuf>>> {
    let mut matches = HashMap::new();
    let mut paths = paths;
    for i in paths.drain(..) {
        let mut file = File::open(&i)?;
        let mut hasher = D::new();
        io::copy(&mut file, &mut hasher)?;
        let h = hasher.result();
        match matches.remove(&h) {
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

fn main() -> io::Result<()> {
    let matches = App::new("rdupes")
        .arg(Arg::with_name("recursive").short("r").takes_value(false))
        .arg(
            Arg::with_name("min_size")
                .long("min-size")
                .takes_value(true),
        )
        .arg(Arg::with_name("directory").required(true))
        .get_matches();
    let dir = matches.value_of_os("directory").unwrap();
    let rec = matches.occurrences_of("recursive") > 0;
    let min_size = if matches.is_present("min_size") {
        value_t!(matches.value_of("min_size"), u64).unwrap_or_else(|e| e.exit())
    } else {
        1
    };
    let table = HashMap::new();
    let mut results = foo(Path::new(dir), table, rec, min_size)?;
    // println!("res: {:?}", results);
    for (_, paths) in results.drain() {
        if paths.len() > 1 {
            let x = bar::<Blake2b>(paths)?;
            for i in x.iter() {
                println!("{:?}", i);
            }
            // println!("{:?}", paths);
        }
    }
    println!("Hello, world!");
    Ok(())
}
