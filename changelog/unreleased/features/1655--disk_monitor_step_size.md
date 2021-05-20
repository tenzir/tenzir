We added a new setting `vast.disk-monitor-step-size` to have the disk monitor
remove N partitions at once before re-checking if the new size of the database
directory is now small enough. This is useful when checking the size of a
directory is an expensive operation itself, e.g. on compressed filesystems.
