# `rdupes`

Command-line tool for finding duplicate files across a number of directory locations.
Has options for controlling recursion and ordering of results for each group of duplicates. Can also limit to only checking for duplicates over a given file size.
It scans files in parallel and will output groups of duplicates as it finishes them (so there is no deterministic ordering of the outputted groups).

```
$ rdupes --max-depth 3 --sort-by depth -r /Users/myuser/Downloads
┌ 585493 bytes
├ /Users/myuser/Downloads/deleuze_control.pdf
└ /Users/myuser/Downloads/deleuze_control (1).pdf

┌ 1212202 bytes
├ /Users/myuser/Downloads/Mike Davis, Marx s Lost Theory, NLR 93, May June 2015.pdf
└ /Users/myuser/Downloads/Mike Davis, Marx s Lost Theory, NLR 93, May June 2015 (1).pdf

┌ 3667978 bytes
├ /Users/myuser/Downloads/KEEP, THE (1983) [1982-04-13] Michael Mann - First Draft.pdf
└ /Users/myuser/Downloads/KEEP, THE (1983) [1982-04-13] Michael Mann - First Draft (1).pdf

┌ 10784439 bytes
├ /Users/myuser/Downloads/kustomize_v3.10.0_darwin_amd64.tar.gz
└ /Users/myuser/Downloads/kustomize_v3.10.0_darwin_amd64 (1).tar.gz

...

57106 regular files seen (of 69586 files total), 895 skipped by min-size (1B). 
50233 total candidate files hashed, 21531 duplicates over 7168 groups. 429186948 wasted bytes.
```

A first pass finds files with identical sizes and then a second pass finds duplicates within those groups using the blake2b hasher.
