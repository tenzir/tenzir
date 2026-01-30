This release includes several bug fixes for the JSON parser, `where`, `replace`, and `if` operators, along with Kafka decompression support and a new `raw_message` option for the `read_syslog` operator.

## 🚀 Features

### Raw message field support for read_syslog operator

The `read_syslog` operator now supports a `raw_message` parameter that preserves the original, unparsed syslog message in a field of your choice. This is useful when you need to retain the exact input for auditing, debugging, or compliance purposes.

When you specify `raw_message=<field>`, the operator stores the complete input message (including all lines for multiline messages) in the specified field. This works with all syslog formats, including RFC 5424, RFC 3164, and octet-counted messages.

For example:

```tql
read_syslog raw_message=original_input
```

This stores the unparsed message in the `original_input` field alongside the parsed structured fields like `hostname`, `app_name`, `message`, and others.

*By @mavam and @claude in #5687.*

## 🐞 Bug fixes

### Fix assertion failure in replace operator when replacing with null

The `replace` operator no longer triggers an assertion failure when using `with=null` on data processed by operators like `ocsf::cast`.

```tql
load_file "dns.json"
read_json
ocsf::cast "dns_activity"
replace what="", with=null
```

*By @mavam and @claude in #5696.*

### Fix intermittent UTF-8 errors in JSON parser

The JSON parser no longer intermittently fails with "The input is not valid UTF-8" when parsing data containing multi-byte UTF-8 characters such as accented letters or emojis.

*By @jachris and @claude in #5698.*

### Fix overzealous constant evaluation in `if` statements

The condition of `if` statements is no longer erroneously evaluated early when it contains a lambda expression that references runtime fields.

*By @jachris in #5701.*

### Support decompression for Kafka operators

Kafka connectors now support decompressing messages with `zstd`, `lz4` and `gzip`.

*By @raxyte and @claude in #5697.*

### Where operator optimization for optional fields

The `where` operator optimization now correctly handles optional fields marked with `?`. Previously, the optimizer didn't account for the optional marker, which could result in incorrect query optimization. This fix ensures that optional field accesses are handled properly without affecting the optimization of regular field accesses.

*By @jachris and @claude.*
