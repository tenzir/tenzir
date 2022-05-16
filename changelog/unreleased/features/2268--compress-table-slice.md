The `segment-store` store backend now compresses data before persisting it,
resulting in 5x space savings for newly archived data, and an up to X% memory
usage reduction. Depending on the speed of the disk this may improve the overall
speed of VAST as well; we measured a negligible less than 1% performance penalty
using the fastest disks we have (5GB/s).
