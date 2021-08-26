# midx-regenerate

This is an advanced tool intended mainly for operators
of large VAST deployments.

## Motivation

In a typical vast database dir, there are two kinds of
files: Partitions and partition synopses. The latter
can be recognized by the ".mdx" file extension. The
data contained in the partition synopses is also
redundanctly stored in the partitions itself, but is usually
read from a separate file as a perfomance optimization.

During startup VAST attempts to construct the "meta index"
out of all the partition synopses. Where the files are
missing, corrupted or outdated they are regenerated from the
data contained.

For large databases, this regeneration process can take a
lot of time. If it is known ahead of time that a bulk
conversion will be required upon the next restart of VAST,
this tool can be used to regenerate the .mdx-files ahead
of time to minimize the downtime.

## Usage

Simply point `mdx-regenerate` to a VAST database directory

    mdx-regenerate /path/to/vast.db
