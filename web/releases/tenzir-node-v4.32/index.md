---
title: "Tenzir Node v4.32: Google SecOps"
slug: tenzir-node-v4.32
authors: [IyeOnline]
date: 2025-04-04
tags: [release, node]
comments: true
---

[Tenzir Node v4.32][github-release] features a new Google SecOps sink operator
and improvements to accessing structured types in TQL.

![Tenzir Node v4.32](tenzir-node-v4.32.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.32.0

<!-- truncate -->

## Google SecOps Integration

[operator-docs]: /next/tql2/operators/to_google_secops

The [`to_google_secops`][operator-docs] operator makes it possible to send events to Google SecOps:

```tql
from {log: "31-Mar-2025 01:35:02.187 client 0.0.0.0#4238: query: tenzir.com IN A + (255.255.255.255)"}
to_google_secops \
  customer_id="00000000-0000-0000-00000000000000000",
  private_key=secret("my_secops_key"),
  client_email="somebody@example.com",
  log_text=log,
  log_type="BIND_DNS",
  region="europe"
```

## Convenient Structure Access

### Lenient Field Access using `.?`

The `.?` operator is a new alternative to the `.` operator that allows field
access without warnings when the field does not exist or the parent record is
`null`.

```tql title="Different forms of record access"
from { }
element1 = this.key                    // Raises a warning
element2 = this.?key                   // Does not raise a warning
element3 = this.key if this.has("key") // Equivalent to the above
```
```tql
{element1: null, element2: null, element3: null}
```

### `record.get()` and `list.get()`

The `get` method on records or lists is an alternative to index expressions that
allows for specifying a default value when the list index is out of bounds or
the record field is missing.

```tql title="Get the first element of a list, or a fallback value"
from (
  {xs: [1, 2, 3]},
  {xs: []},
}
first = xs.get(0, -1)
```

```tql
{first: 1}
{first: -1}
```

```tql title="Access a field of a record, or a fallback value"
from (
  {x: 1, y: 2},
  {x: 3},
}
x = x.get("x", -1)
y = y.get("y", -1)
```

```tql
{x: 1, y: 2}
{x: 3, y: -1}
```

#### Graceful Mappings

The new `get` function allows you to write mappings that gracefully handle
failures without raising (potentially multiple) warnings.

```tql title="Map country tags to country names"
let $country_tag_to_name = {
  ger: "Germany",
  fra: "France",
  ind: "India"
}
let $fallback = "unknown"

from (
  {country_tag: "ger"},
  {country_tag: "ind"},
  {country_tag: "ita }
)
country = $country_tag_to_name.get(country_tag, $fallback)
```
```tql
{country_tag: "ger", country: "Germany"}
{country_tag: "ind", country: "India"}
{country_tag: "ita", country: "unknown"}
```

### Indexing records with integers

Indexing expressions on records now support numeric indices to access record
fields. For example, `this[0]` returns the first field of the top-level record.

### More powerful `has`

The `has` method on records no longer requires the field name to be a constant.

```tql
from {x: "hello", fieldname: "x"}
r = x.has(fieldname)
```
```tql
{x: "hello", fieldname: "x", r: true}
```

## New Way to Access the Config

The `config` function replaces the previous `config` operator as a more flexible
mechanism to access variables from the configuration file.

```tql title="Get all user defined operators"
from config()
this = tenzir.operators
unroll this
```

:::info `config` operator has been removed
If you rely on the old operator, you can use `from config()` as a replacement.
:::

## Fixes, Improvements & Other Small Changes

This release also contains a number of small fixes and improvements, which you
can find in the [changelog][changelog].

## Let's Connect

Do you want to directly engage with Tenzir? Join our [Discord server][discord],
where we discuss projects and features and host our bi-weekly office hours
(every second Tuesday at 5 PM CET). Regardless of whether you just want to hang
out or have that one very specific question you just need answered, you are always
welcome!

[discord]: /discord
[changelog]: /changelog#v4310
