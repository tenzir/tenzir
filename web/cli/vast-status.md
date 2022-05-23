The `status` command dumps VAST's runtime state in JSON format.

The unit of measurement for memory sizes is bytes.

For example, to see how many events of each type are indexed, this command can
be used:

```
vast status --detailed | jq '.index.statistics.layouts'
```
