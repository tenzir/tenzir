---
title: "`read_delimited` and `read_delimited_regex`"
type: feature
author: dominiklohmann
created: 2025-06-23T09:02:07Z
pr: 5291
---

TQL now supports two new operators for parsing data streams with custom
delimiters: `read_delimited` and `read_delimited_regex`. These operators provide a more
intuitive and discoverable way to split data on custom separators compared to
the deprecated `split_at_regex` option in `read_lines`.

The `read_delimited` operator splits input on exact string or blob matches:

```tql
load_file "data.txt"
read_delimited "||"
```

The `read_delimited_regex` operator splits input using regular expression patterns:

```tql
load_tcp "0.0.0.0:514" {
  read_delimited_regex "(?=<[0-9]+>)"
}
```

Both operators support binary data processing and optionally including the
separator in the output. The `split_at_regex` option in `read_lines` is now
deprecated in favor of these dedicated operators.
