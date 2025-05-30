Tenzir nodes sometimes failed when trying to canonicalize file system paths
before opening them when the disk-monitor or compaction rotated them out. This
is now handled gracefully.
