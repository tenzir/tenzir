Metrics events now optionally contain a metadata field that is a key-value
mapping of string to string, allowing for finer-grained introspection. For now
this enables correlation of metrics events and individual queries. A set of new
metrics for query lookup use this feature to include the query ID. I.e.:
```
{
  "ts": "2021-12-09T12:47:09.079148669",
  "version": "2021.11.18-269-gba9f97bf77-dirty",
  "actor": "meta-index",
  "key": "meta-index.lookup.runtime",
  "value": 0.070954,
  "metadata": {
    "query": "7E18BF00-0C8C-4841-B8A4-C8EAEB9E9203"
  }
}
```
