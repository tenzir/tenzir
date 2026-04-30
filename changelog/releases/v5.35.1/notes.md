This release restores correct retention for metrics and diagnostics in mixed-age partitions and brings back actionable TLS hints when ClickHouse connections fail due to a TLS/plaintext mismatch.

## 🐞 Bug fixes

### ClickHouse TLS mismatch diagnostics

ClickHouse connection errors caused by TLS/plaintext mismatches now include the TLS notes and hint again. This helps identify when `to_clickhouse` is using TLS against a plaintext ClickHouse endpoint and suggests setting `tls=false` when appropriate.

*By @mavam and @codex in #6098.*

### Retention for mixed-age metrics partitions

Default retention policies now continue deleting metrics and diagnostics as their timestamps age into the retention window, even when older and newer events share a partition.

Previously, a partition that still contained newer events after retention could be skipped by later retention runs, leaving those events behind after they expired.

*By @tobim and @codex in #6086.*
