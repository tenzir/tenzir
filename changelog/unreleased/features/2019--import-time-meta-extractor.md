The `#import_time` meta extractor allows for querying events based on the time
they arrived at the VAST server process. It may only be used for comparisons
with [time value
literals](https://docs.tenzir.com/vast/query-language/values/#time), e.g., `vast
export json '#import_time > 1 hour ago'` exports all events that were imported
within the last hour as NDJSON.
