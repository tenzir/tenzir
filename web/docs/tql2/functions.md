# Functions

Tenzir comes with a wide range of built-in functions.

## Networking

Function | Description | Example
:--------|:-------------|:-------
[`community_id`](./functions/community_id.md) | Computes a Community ID | `community_id(src_ip=1.2.3.4, dst_ip=4.5.6.7, proto="tcp")`
[`is_v4`](functions/is_v4.md) | Checks if an IP is IPv4 | `is_v4(1.2.3.4)`
[`is_v6`](functions/is_v6.md) | Checks if an IP is IPv6 | `is_v6(::1)`

<!--
## TODO?
- `random`
- `type_id`

## OCSF
- `ocsf::category_name`
- `ocsf::category_uid`
- `ocsf::class_name`
- `ocsf::class_uid`

## Paths
- `file_name`
- `parent_dir`

## Conversion
- `ip`
- `int`
- `uint`
- `time`

## Aggregation (?)
- `count`
- `quantile`
- `sum`

## Numeric (?)
- `sqrt`
- `round`

## ???
- `env`
- `secret`

## Time and Duration
- `now`
- `as_secs`
- `since_epoch`

## Strings
- `capitalize`
- `ends_with`
- `is_alnum`
- `is_alpha`
- `is_lower`
- `is_numeric`
- `is_printable`
- `is_title`
- `is_upper`
- `length_bytes`
- `length_chars`
- `starts_with`
- `to_lower`
- `to_title`
- `to_upper`
- `trim`
- `trim_end`
- `trim_start`

## Lists
- `length`

## Records
- `has`

## List and String ?
- `reverse`

## TODO
- `grok`
- `parse_cef`
- `parse_json`
-->
