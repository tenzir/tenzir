The `count --estimate` command no longer loads store files from disk, resulting
in a significant performance improvement. It now only loads the relevant index
files.
