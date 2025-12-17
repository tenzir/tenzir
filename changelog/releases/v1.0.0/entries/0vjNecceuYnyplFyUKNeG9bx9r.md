---
title: "Introduce the `#import_time` meta extractor"
type: feature
author: dominiklohmann
created: 2022-01-07T12:00:56Z
pr: 2019
---

The `#import_time` meta extractor allows for querying events based on the time
they arrived at the VAST server process. It may only be used for comparisons
with [time value
literals](https://vast.io/docs/understand/query-language/expressions#values),
e.g., `vast export json '#import_time > 1 hour ago'` exports all events that
were imported within the last hour as NDJSON.
