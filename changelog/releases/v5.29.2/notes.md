This patch release fixes several correctness and performance issues across parsing, querying, and storage, and completes Suricata 8 schema coverage.

## 🚀 Features

### Add store origin metadata to feather files

Feather store files now include a `TENZIR:store:origin` key in the Arrow table schema metadata. The value is `"ingest"` for freshly ingested data, `"rebuild"` for partitions created by the rebuild command, and `"compaction"` for partitions created by the compaction plugin. This allows external tooling such as `pyarrow` to distinguish how a partition was produced.

*By @tobim.*

### Install Tenzir via Homebrew on macOS

You can now install Tenzir on Apple Silicon macOS via Homebrew:

```sh
brew tap tenzir/tenzir
brew install --cask tenzir
```

You can also install directly without tapping first:

```sh
brew install --cask tenzir/tenzir/tenzir
```

The release workflow keeps the Homebrew cask in sync with the signed macOS package so installs and uninstalls stay current across releases.

*By @mavam in #5876.*

## 🔧 Changes

### Add Suricata schema types for IKE, HTTP2, PGSQL, and Modbus

The bundled Suricata schema now covers the remaining event types listed in the Suricata 8.0.3 EVE JSON format documentation: IKE (IKEv1/IKEv2), HTTP/2, PostgreSQL, and Modbus. This completes Suricata 8 schema coverage for Tenzir.

*By @tobim in #5914.*

### Correct AWS Marketplace container image

The AWS Marketplace ECR repository `tenzir-node` was incorrectly populated with the `tenzir` image. It now correctly ships `tenzir-node`, which runs a Tenzir node by default.

If you relied on the previous behavior, you can restore it by setting `tenzir` as a custom entrypoint in your ECS task definition.

*By @lava in #5925.*

## 🐞 Bug fixes

### Fix batch timeout to flush asynchronously

The batch timeout was only checked when a new event arrived, so a single event followed by an idle stream would never be emitted. The timeout now fires independently of upstream activity.

*By @aljazerzen in #5906.*

### Fix over-reservation in partition_array for string/blob types

Splitting Arrow arrays for string and blob types no longer over-reserves memory. Previously both output builders reserved the full input size each, using up to twice the necessary memory.

*By @jachris in #5899.*

### Fix parse_winlog batch splitting

The `parse_winlog` function could fragment output into thousands of tiny batches due to type conflicts in `RenderingInfo/Keywords`, where events with one `<Keyword>` emitted a string but events with multiple emitted a list. Additionally, `EventData` with unnamed `<Data>` elements is now always emitted as a record with `_0`, `_1`, etc. as field names instead of a list.

*By @jachris in #5901.*

### Fix pattern equality ignoring case-insensitive flag

Pattern equality checks now correctly consider the case-insensitive flag. Previously, two patterns that differed only in case sensitivity were treated as equal, violating the hash/equality contract.

*By @jachris in #5900.*

### Fix secret comparison bypass in `in` operator fast path

The `in` operator fast path now correctly prevents comparison of secret values. Previously, `secret_value in [...]` would silently compare instead of returning null with a warning, bypassing the established secret comparison policy.

*By @jachris in #5899.*

### Optimize `in` operator and fix eq/neq null semantics

The `in` operator for list expressions is up to 33x faster. Previously it created and finalized entire Arrow arrays for every element comparison, causing severe overhead for expressions like `EventID in [5447, 4661, ...]`.

Additionally, comparing a typed null value with `==` now returns `false` instead of `null`, and `!=` returns `true`, fixing a correctness issue with null handling in equality comparisons.

*By @jachris in #5899.*

### Support long syslog structured-data parameter names

The `read_syslog` operator and `parse_syslog` function now accept RFC 5424 structured-data parameter names longer than 32 characters, which some vendors emit despite the specification limit.

For example, this message now parses successfully instead of being rejected:

```text
<134>1 2026-03-18T11:00:51.194137+01:00 HOSTNAME abc 9043 23003147 [F5@12276 thx_f5_for_ignoring_the_32_char_limit_in_structured_data="thx"] broken example
```

This improves interoperability with vendor syslog implementations that exceed the RFC limit for structured-data parameter names.

*By @mavam and @codex.*
