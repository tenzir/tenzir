---
title: "Add the import zeek-json command"
type: feature
author: dominiklohmann
created: 2021-01-06T14:08:38Z
pr: 1259
---

The new `import zeek-json` command allows for importing line-delimited Zeek JSON
logs as produced by the
[json-streaming-logs](https://github.com/corelight/json-streaming-logs) package.
Unlike stock Zeek JSON logs, where one file contains exactly one log type, the
streaming format contains different log event types in a single stream and uses
an additional `_path` field to disambiguate the log type. For stock Zeek JSON
logs, use the existing `import json` with the `-t` flag to specify the log type.
