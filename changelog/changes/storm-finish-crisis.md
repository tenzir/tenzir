---
title: "`read_until` and `read_until_regex`"
type: feature
authors: dominiklohmann
pr: 5291
---

TQL now supports two new operators for parsing data streams with custom
delimiters: `read_until` and `read_until_regex`. These operators provide a more
intuitive and discoverable way to split data on custom separators compared to
the deprecated `split_at_regex` option in `read_lines`.

The `read_until` operator splits input on exact string or blob matches:

```tql
load_file "data.txt"
read_until "||"
```

The `read_until_regex` operator splits input using regular expression patterns:

```tql
load_tcp "0.0.0.0:514" {
  read_until_regex "(?=<[0-9]+>)"
}
```

Both operators support binary data processing and optionally including the
separator in the output. The `split_at_regex` option in `read_lines` is now
deprecated in favor of these dedicated operators.
