---
title: Splunk HEC acknowledgements
type: feature
authors:
  - mavam
created: 2026-08-26T09:51:33.726804Z
---

The `accept_splunk` operator now supports the HEC acknowledgement protocol:

```tql
accept_splunk hec_token=secret("splunk-hec-token"), ack=true
```

HEC clients receive an `ackId` for each accepted request and can query its
status through `/services/collector/ack`. An acknowledgement becomes ready
after a checkpoint containing the complete request commits, so restored
pipelines can preserve delivery guarantees across node crashes. IDs remain
queryable for 10 minutes by default, making retries safe while keeping
abandoned requests from exhausting the acknowledgement limit.
