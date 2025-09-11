---
title: "SentinelOne Singularity Data Lake Integration"
type: feature
authors: IyeOnline
pr: 5455
---

We have added an integration for the SentinelOne Singularity™ Data Lake!

The new `to_sentinelone_data_lake` operator allows you to easily send structured
and unstructured events to the data lake:

```tql
subscribe "sentinelone-data-lake"
to_sentinelone_data_lake "https://ingest.eu1.sentinelone.net",
  token=secret("sentinelone-token")
```
