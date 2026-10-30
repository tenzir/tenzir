This release stops trusting certificates from eMudhra intermediate certificate authorities whose private keys leaked. It also makes to_iceberg write fewer small data files and lets SentinelOne evaluate more of a filter's conditions on its side.

## 🔧 Changes

### Fewer data files from to_iceberg

The `to_iceberg` operator now writes every column from the current table schema into each data file, filling missing columns with typed nulls. Mixed event schemas can share a file when previously empty columns gain values, reducing small files.

The operator also retains buffered batches when nullable fields are added, allowing several schema additions to share one data file. Open streaming files still close on a schema change. Type changes and partition changes also close the affected buffers before adopting the new layout.

*By @zedoraps.*

### More SentinelOne filters pushed inside disjunctions

Pipelines that filter `from_sentinelone_data_lake` with a disjunction of conjunctions now send more of that filter to SentinelOne. Previously, a single condition without a PowerQuery equivalent, such as a comparison with an IP address, kept the entire disjunction local. Now SentinelOne evaluates the rest of each branch, while Tenzir still applies the complete filter locally:

```tql
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-console-token")
where (severity == 5 and src_ip == 10.0.0.1) or severity == 6
```

Here, SentinelOne now filters for `severity == 5 or severity == 6`, where it previously received no filter at all.

*By @mavam.*

## 🐞 Bug fixes

### Distrust the leaked eMudhra CA keys

The container image no longer trusts certificates issued by eMudhra intermediate CAs whose private keys leaked. It now ships the NSS 3.131.1 root store, and the root stores bundled with gRPC and LibreSSL point at that same store.

*By @lava.*
