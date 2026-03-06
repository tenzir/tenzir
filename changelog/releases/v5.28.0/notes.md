This release adds support for parsing Check Point syslog structured-data dialects that deviate from RFC 5424, improving out-of-the-box interoperability with Check Point exports. It also makes DNS hostname resolution in the load_tcp operator opt-in and fixes several parser bugs related to schema changes between events.

## 🚀 Features

### Check Point syslog structured-data dialect parsing

`parse_syslog()` and `read_syslog` now accept common Check Point structured-data variants that are not strictly RFC 5424 compliant. This includes `key:"value"` parameters, semicolon-separated parameters, and records that omit an SD-ID entirely.

For records without an SD-ID, Tenzir now normalizes the structured data under `checkpoint_2620`, so downstream pipelines can use a stable field path.

For example, the message `<134>1 ... - [action:"Accept"; conn_direction:"Incoming"]` now parses successfully and maps to `structured_data.checkpoint_2620`. This improves interoperability with Check Point exports and reduces ingestion-time preprocessing.

*By @mavam and @codex in #5851.*

## 🔧 Changes

### DNS hostname resolution opt-in for load_tcp operator

The `load_tcp` operator now makes DNS hostname resolution opt-in with the `resolve_hostnames` parameter (defaults to `false`).

Previously, the operator always attempted reverse DNS lookups for peer endpoints, which could fail in environments without working reverse DNS configurations. Now you can enable this behavior by setting `resolve_hostnames` to `true`:

```tql
load_tcp endpoint="0.0.0.0:5555" resolve_hostnames=true {
  read_json
}
```

When enabled and DNS resolution fails, the operator emits a warning diagnostic (once) instead of failing. This allows the operator to continue functioning in environments where reverse DNS is unavailable or unreliable.

*By @tobim and @codex in #5865.*

### JSON parse error context

JSON parsing errors now display the surrounding bytes at the error location. This makes it easier to diagnose malformed JSON in your data pipelines.

For example, if your JSON is missing a closing bracket, the error message shows you the bytes around that location and marks where the parser stopped expecting more input.

*By @IyeOnline in #5805.*

## 🐞 Bug fixes

### Parser bug fixes for schema changes

Fixed multiple issues that could cause errors or incorrect behavior when the schema of parsed events changes between records. This is particularly important when ingesting data from sources that may add, remove, or modify fields over time.

Schema mismatch warnings for repeated fields in JSON objects (which Tenzir interprets as lists) now include an explanatory hint, making it clearer what's happening when a field appears multiple times where a single value was expected.

*By @IyeOnline in #5805.*

### Uncaught exception reporting

We improved the reporting for unexpected diagnostics outside of operator execution, such as during startup. In these cases you will now get the diagnostic message.

*By @IyeOnline in #5805.*
