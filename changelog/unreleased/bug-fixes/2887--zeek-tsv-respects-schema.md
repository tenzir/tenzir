The Zeek TSV reader now respects the schema files in the bundled `zeek.schema`
file, and produces data of the same schema as the Zeek JSON reader. E.g.,
instead of producing a top-level ip field `id.orig_h`, the reader now produces a
top-level record field `id` that contains the ip field `orig_h`, effectively
unflattening the data.
