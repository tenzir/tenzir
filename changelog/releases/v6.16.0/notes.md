Tenzir now matches stock Sigma rules against Open Cybersecurity Schema Framework events automatically and collects Microsoft 365 audit activity. This release also improves query performance, expands identity federation support, and fixes issues with Sigma matching, context loading, memory metrics, and automatic rebuilds.

## 🚀 Features

### `from_clickhouse` pushes filters, limits, and field selection into SQL

The `from_clickhouse` operator now translates the pipeline that follows it into the `SELECT` it sends to ClickHouse. A `where` becomes a `WHERE` clause, a `select` narrows the selected columns, and a `head` adds a `LIMIT`, so ClickHouse reads and transfers only what the pipeline needs. For example,

```tql
from_clickhouse table="logs.events"
where severity > 3 or source == "fw" and code in [401, 403]
select id, message
head 100
```

sends a query equivalent to:

```sql
SELECT id, message, severity, source, code
FROM logs.events
WHERE (severity > 3 OR (source = 'fw' AND code IN (401, 403)))
LIMIT 100
```

The translation only covers predicates whose ClickHouse semantics match TQL exactly: comparisons of a column with a literal of the same kind (numbers, strings, booleans), `null` checks, `in` with a list of literals, and `and`, `or`, and `not`. Nullable columns receive a null guard so that `not (x == 1)` keeps rows where `x` is `null`, as it does in TQL. Nested fields address tuple elements. Anything else, such as function calls, comparisons against `time` or `ip` values, or columns with `Enum` or `Decimal` types, keeps running locally with unchanged results. When you pass `sql` instead of `table`, the query is sent as is.

*By @mavam.*

### Automatic OCSF matching for Sigma rules

Stock Sigma rules now match conformant OCSF events automatically. The `sigma` operator recognizes OCSF-shaped table schemas, plans matching once per schema, and preserves source-field semantics such as process identity, Windows users, registry values, DNS answers, integrity levels, and structured hashes. A built-in mapping catalog covers the major SigmaHQ logsources: every Sysmon-backed Windows category, Linux and macOS process creation, the Windows Security, System, PowerShell, and Defender channels, Okta, Entra ID, Azure activity logs, AWS CloudTrail, GCP Cloud Audit Logs, and Zeek DNS, HTTP, and SMB. Four out of five rules in the SigmaHQ corpus translate completely; rules over source fields that OCSF does not preserve are skipped with a diagnostic that names the field:

```tql
subscribe "ocsf"
sigma path="sigma/rules"
```

An OCSF-shaped schema has a string `metadata.version` and an `int64` or `uint64` `class_uid`. Other schemas keep direct field matching. Unknown fields fall back to literal schema fields, while rules with unresolved fields are skipped safely. Use `mapping="direct"` to opt out of structural recognition and match every rule field literally. Output formats and original source evidence remain unchanged.

*By @mavam.*

### Blob conversion function

The `blob` function converts UTF-8 strings to blobs without encoding the payload first. Use it when you need to retain serialized data as a blob:

```tql
extra_data = blob(extra_data.print_ndjson())
```

*By @tobim.*

### Collect Microsoft 365 activity

Use the new `from_microsoft_365_activity` operator to collect unified audit records from Microsoft 365. It handles Activity API subscriptions, bounded backfills, continuous polling, pagination, retries, and snapshot-backed blob checkpoints.

For example, collect general audit events as they become available:

```tql
from_microsoft_365_activity auth={
    tenant_id: secret("m365-tenant-id"),
    client_id: secret("m365-client-id"),
    client_secret: secret("m365-client-secret"),
  },
  content_types=["Audit.General"]
```

The operator emits the original audit fields together with source metadata:

```tql
{
  Id: "8f18c3d0-…",
  Operation: "UserLoggedIn",
  microsoft_365_activity: {
    tenant_id: "7f44c930-…",
    content_type: "Audit.General",
    content_id: "20260902123456789000$…",
    content_created: 2026-09-02T12:34:56Z,
  },
}
```

*By @mavam.*

### Decode Kafka Avro with Schema Registry

The `from_kafka` operator now decodes Avro message values directly with `schema_registry="https://registry.example.com"`. It supports Confluent schema IDs in payload prefixes and schema GUIDs in Kafka headers, caches writer schemas, and resolves named schema references. Registry authentication and TLS settings are independent of Kafka broker settings.

Registry and decoding failures stop consumption without committing the failed batch or later offsets. Kafka tombstones are skipped and still count towards `count`.

*By @raxyte.*

### Entra workload identity federation for Azure Blob Storage

`from_azure_blob_storage` and `to_azure_blob_storage` now take an `azure_auth` record with a `web_identity` field, so a single node can read from and write to blobs in several Microsoft Entra tenants at once:

```tql
from_azure_blob_storage "abfss://logs@customer1.dfs.core.windows.net/**.json.gz",
  azure_auth={
    tenant_id: secret("customer1-entra-tenant"),
    client_id: secret("customer1-entra-client"),
    web_identity: {
      token_endpoint: {
        url: env("ACTIONS_ID_TOKEN_REQUEST_URL"),
        query_params: {"audience": "api://AzureADTokenExchange"},
        headers: {
          "Authorization": "Bearer " + env("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
        },
        path: ".value",
      },
    },
  } {
  read_json
}
```

The OIDC token can come from a `token_endpoint`, a `token_file`, or an inline `token`, which covers GitHub Actions, the GCP metadata server, and Kubernetes projected service account tokens.

Previously workload identity federation was only reachable through the process-global `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_FEDERATED_TOKEN_FILE` environment variables, which pinned the whole node to one tenant. `azure_auth` also accepts `client_secret` instead of `web_identity`, and cannot be combined with `account_key`.

*By @lava.*

### Limit and field selection pushdown toward sources

The pipeline optimizer now propagates the row limit of `head` and the field selection of `select` toward the source of a pipeline, alongside the predicate pushdown it already performs for `where`. Sources that understand these hints can stop reading early and skip fields that the pipeline never uses.

For example, in

```tql
export
where severity == "high"
select id, message
head 10
```

the source learns that at most 10 matching events are needed and that only `id`, `message`, and `severity` are read. The `head` and `select` operators stay in the pipeline, so results are unchanged whether or not a source acts on the hints. `export` also records the field selection, which you can inspect with `tenzir --dump-opt-ir`; acting on it at the storage layer is not planned.

The `export` operator now honors the limit: it stops opening partitions once it has enough matching events. Filters that require local evaluation conservatively read everything. The `subscribe` source now acts on these hints: it stops after the requested number of matching events and drops unneeded top-level fields before forwarding events. Nested field selections retain the containing record until `select` applies the exact selection.

The `read_parquet` and `read_feather` operators honor both hints, including inside an explicit `from_file` subpipeline. They retain filter-required fields, skip decoding unrelated top-level columns, and stop after enough matching rows. Feather supports this for IPC files and streams, including concatenated streams with different column orders. Filters containing function calls conservatively retain all columns.

Parquet still buffers its full byte input before reading the footer. These optimizations reduce decoding work, not the bytes fetched from the source; random-access file I/O is not part of `read_parquet`.

*By @mavam.*

### Query parameters for web identity token endpoints

`web_identity.token_endpoint` now takes a `query_params` record, so the audience an OIDC provider expects no longer has to be concatenated onto the URL by hand.

Keys and values are percent-encoded and joined onto whatever query the URL already carries, which matters for GitHub Actions: its `ACTIONS_ID_TOKEN_REQUEST_URL` already ends in an `api-version` parameter. Both `string` and `secret` values are accepted, like `headers`.

```tql
from_azure_blob_storage "abfss://container@account.dfs.core.windows.net/blob.json",
  azure_auth={
    tenant_id: secret("entra-tenant-id"),
    client_id: secret("entra-client-id"),
    web_identity: {
      token_endpoint: {
        url: env("ACTIONS_ID_TOKEN_REQUEST_URL"),
        query_params: { "audience": "api://AzureADTokenExchange" },
        headers: {
          "Authorization": "Bearer " + env("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
        },
        path: ".value",
      },
    },
  } {
  read_json
}
```

Entra always expects `api://AzureADTokenExchange`, but the parameter is not Azure-specific: AWS wants `sts.amazonaws.com` there, and other providers name the parameter something else entirely.

*By @lava.*

## 🔧 Changes

### Faster match-all catalog lookups

Catalog lookups for queries that match all events, such as an unfiltered `export` or the candidate selection of a rebuild, no longer evaluate the query against every partition's synopses. On state directories with many partitions this makes such lookups start noticeably faster.

*By @tobim.*

### Sigma diagnostics point at the rule

Diagnostics about one inline Sigma rule now underline that rule's `title` line inside the `rules` string literal instead of the complete literal. This covers rules the operator ignores, rules with duplicate identities, and rules skipped for OCSF input, whether the rules come as one string or as a list of strings. The rule's line is located for raw strings and for plain strings without escapes; a plain string with escapes, or a value computed by an expression, keeps the diagnostic on the complete literal rather than on a shifted line.

*By @mavam.*

### Source locations in node startup diagnostics

Diagnostics that a node emits for configured operators and for pipelines that ship with a package now name the file or pipeline they come from.

Previously a broken `tenzir.operators` definition or package pipeline produced only the bare message:

```
error: operator `wherex` not found
  = hint: did you mean `where`?
```

Now the offending definition is pointed at directly, both on the console and in the node log:

```
error: operator `wherex` not found
 --> tenzir.operators.myop:1:10
  |
1 | head 5 | wherex true
  |          ^^^^^^
  = hint: did you mean `where`?
```

Errors from packages that fail to load also appear in the node log with their source location and notes instead of an unstructured dump.

Diagnostics whose location lies inside a user-defined operator that the pipeline expanded now point into the operator's own definition and name the invocation as the call site, instead of at an unrelated span of the pipeline that used it:

```
error: operator `nonexistent` not found
 --> /etc/tenzir/packages/acme/operators/mark.tql:1:1
  |
1 | nonexistent
  | ^^^^^^^^^^^
  |
 --> <packages/acme/enrich>:2:1
  |
2 | acme::mark
  | ^^^^^^^^^^ called from here
```

*By @aljazerzen and @claude.*

## 🐞 Bug fixes

### Automatic rebuilds merge undersized partitions again

Automatic rebuilds consolidate undersized partitions again. Since v6.15.0, the rebuilder silently skipped every batch whose merged result would still fall below 80% of `tenzir.max-partition-size`. On nodes where the rebuild memory budget limits how many partitions fit into one batch, this stopped consolidation entirely: a run would report tens of thousands of candidates, rebuild only a handful of partitions, and rescan the same candidates on every subsequent run. The rebuilder now merges every batch that contains more than one partition, and only skips lone partitions that have nothing to merge with.

*By @tobim.*

### Memory metrics no longer stall the node

The `memory` metrics collector no longer reads `/proc/self/smaps_rollup` every second. Producing that file makes the kernel walk every page table entry of the process while holding its memory-map lock. On nodes with a large resident set each read took a substantial fraction of a second, and during that time every concurrent `mmap`, `munmap`, page fault, and allocator purge in the process had to wait. Under load this serialized the whole node behind a single thread, leaving all but one core idle while throughput collapsed.

The `procfs.smaps` record of `tenzir.metrics.memory` is gone. Its resident, anonymous, and swap totals duplicated the `vm_rss_bytes`, `rss_anon_bytes`, and `vm_swap_bytes` counters that `procfs.status` already reports from `/proc/self/status`, which the kernel maintains incrementally and which cost microseconds to read. The remaining value, `hugetlb_bytes`, now lives in `procfs.status` as well. The fields `pss_bytes`, `private_clean_bytes`, and `private_dirty_bytes` have no cheap source and were removed without replacement.

*By @tobim.*

### Reliable loading of large context states

`context_load` now correctly reassembles context state that arrives in multiple chunks. Previously, loading could discard all but the final chunk of the input, which made loading larger context states—such as a DCSO Bloom filter fed through `from_file` and `decompress_gzip`—fail with errors like `invalid version` or `bloom filter buffer too small`, depending on how the byte stream happened to be chunked. The same file could load on one machine and fail on another.

*By @tobim.*

### Sigma filters on missing fields no longer suppress matches

The `sigma` operator evaluated detection items in three-valued logic, so a filter over a field the event does not carry produced `null`, and `not null` stayed `null`. Rules using the common `selection and not filter_*` pattern never matched events that lacked the filter's field. Items over missing fields now evaluate to `false`, so negated filters pass as Sigma specifies.

*By @mavam.*

### Sigma regular expressions match anywhere in the value

The `sigma` operator evaluated the `re` modifier as a full match, so a stock rule such as `RemoteName|re: '://[0-9]{1,3}\\.[0-9]{1,3}'` never fired unless the pattern happened to describe the entire value. Sigma defines `re` without implicit anchors. The operator now matches regular expressions anywhere in the value; explicit `^` and `$` anchors keep their meaning, and the patterns derived from plain strings and wildcards are unaffected because they spell out their anchors.

*By @mavam.*
