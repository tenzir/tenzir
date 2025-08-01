---
title: "Replacing values"
type: feature
authors: raxyte
pr: 5372
---

The new `replace` operator allows you to find and replace all occurrences of a
specific value across all fields in your data with another value. This is
particularly useful for data sanitization, redacting sensitive information, or
normalizing values across datasets.

The operator scans every field in each input event and replaces any value that
equals the `what` parameter with the value specified by `with`.

###### Examples

Replace all occurrences of the string `"-"` with null:

```tql
replace what="-", with=null
```

Redact a specific IP address across all fields:

```tql
replace what=192.168.1.1, with="REDACTED"
```

Replace boolean values to standardize data:

```tql
replace what=true, with="yes"
```
