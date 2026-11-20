---
title: Zeek TSV round trip with write_zeek_tsv and read_zeek_tsv
type: bugfix
authors:
  - mavam
created: 2026-11-17T07:34:32.581256Z
---

The output of `write_zeek_tsv` now reads back with `read_zeek_tsv`. Previously,
reading a log with a `time` or `interval` column failed, because
`write_zeek_tsv` did not write these values as Zeek does. It now writes times
and intervals as seconds, as in `1794812400.123456`, omits the `zeek.` prefix of
the schema name in `#path`, and ends the log with a `#close` line.

Numbers that round up to the next whole number, such as `0.99999999996`, no
longer print as `0.1` in `write_lines`, and intervals such as `0.9999996s`
no longer print as `0.100000` in `write_zeek_tsv`.
