The `status` command dumps VAST's runtime state in JSON format.

For example, to see how many events of each type are indexed, this command can
be used:

```
vast status | jq '.node.index.statistics.layouts'
```
