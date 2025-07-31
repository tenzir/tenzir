---
title: "Trimming custom characters"
type: feature
authors: mavam
pr: 5389
---

The `trim()`, `trim_start()`, and `trim_end()` functions can now remove
specific characters from strings, not just whitespace. Pass a second argument
containing a string where each character represents a character to remove:

```tql
from {
  path: "/path/to/file/".trim("/"),
  decorated: "--hello--world--".trim("-"),
  complex: "/-/data/-/".trim("/-")
}
```

```tql
{
  path: "path/to/file",
  decorated: "hello--world",
  complex: "data"
}
```

Each character in the second argument is treated individually, not as a complete
string to match:

```tql
from {
  // Removes 'a', 'e', and 'g' from both ends
  chars: "abcdefg".trim("aeg"),
  // Removes any 'h', 'e', 'l', or 'o' from both ends
  word: "helloworldhello".trim("hello")
}
```

```tql
{
  chars: "bcdf",
  word: "wr"
}
```

This also works with `trim_start()` and `trim_end()` for one-sided trimming:

```tql
from {
  start: "///api/v1/users".trim_start("/"),
  end: "data.csv.tmp.bak".trim_end(".tmpbak")
}
```

```tql
{
  start: "api/v1/users",
  end: "data.csv"
}
```
