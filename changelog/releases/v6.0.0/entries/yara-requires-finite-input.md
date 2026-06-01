---
title: '`yara` requires finite input'
type: breaking
author: mavam
pr: 6035
created: 2026-04-30T13:01:54.70296Z
---

The `yara` operator no longer accepts the `blockwise` argument. Instead, it
buffers the entire input as one contiguous byte sequence and runs the YARA
scan when the input ends. Matches can therefore span chunk boundaries, but
`yara` is now only suitable for finite byte streams. Don't use it on
never-ending inputs.

The `rule` argument now also accepts a single string in addition to a list
of strings:

```tql
from_file "evil.exe", mmap=true {
  yara "rule.yara"
}
```

Removed:

```tql
yara ["rule.yara"], blockwise=true
```
