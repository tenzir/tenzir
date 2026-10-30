---
title: More Splunk filters pushed into searches
type: feature
authors:
  - mavam
created: 2026-10-30T14:07:33.000000Z
---

The `from_splunk` operator now also sends negations and IP address filters to
Splunk:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where user != "root" and src_ip in 10.0.0.0/8
```

Splunk receives this search:

```spl
search index="main" (src_ip="10.0.0.0/8" OR src_ip="::ffff:10.0.0.0/104")
| where (isnull(user) OR mvcount(user)>1 OR if(user=="root", false(), true()))
| fields *
```

Negations go into a `where` stage that keeps events without the field and
events where the field has several values, as TQL does, and compares the
remaining values exactly. Equality and membership with `!=`, `not`, and
`not in` reach Splunk for strings, numbers, and booleans, and so do IP address
equality, address lists, and networks, also with `ip()`.

Negations of numbers and booleans stay in Tenzir with `raw=true`, since Splunk's
strings then differ from the numbers and booleans in the filter. So does a
negated string that Tenzir would infer as another type, such as `"5"`.
