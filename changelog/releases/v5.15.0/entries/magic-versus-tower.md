---
title: "Improve `to_splunk` composability"
type: feature
author: IyeOnline
created: 2025-09-18T11:21:54Z
pr: 5478
---

We have improved the composability of the `to_splunk` operator. The `host` and
`source` parameters now accept a `string`-expression instead of only a constant.
Further, there is a new `event` parameter that can be used to specify what should
be send as the event to the Splunk HTTP Event Collector.

The combination of these options improves the composability of the operator,
allowing you to set event-specific Splunk parameters, while not also transmitting
them as part of the actual event:

```tql
from {
  host: "my-host",
  a: 42,
  b: 0
}

// move the entire event into `event`
this = { event: this }

// hoist the splunk specific field back out
move host = event.host

to_splunk "https://localhost:8088",
  hec_token=secret("splunk-hec-token"),
  host=host,
  event=event
```
