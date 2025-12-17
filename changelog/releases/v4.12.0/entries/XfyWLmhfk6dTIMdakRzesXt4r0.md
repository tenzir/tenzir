---
title: "Add a delay to retrying failed pipelines"
type: feature
author: Dakostu
created: 2024-04-17T14:53:45Z
pr: 4108
---

Stopping a failed pipeline now moves it into the stopped state in the app and
through the `/pipeline/update` API, stopping automatic restarts on failure.

Pipelines now restart on failure at most every minute. The new API parameter
`retry_delay` is available in the `/pipeline/create`, `/pipeline/launch`, and
`/pipeline/update` APIs to customize this value. For configured pipelines, the
new `restart-on-error` option supersedes the previous `autostart.failed` option
and may be set either to a boolean or to a duration, with the former using the
default retry delay and the latter using a custom one.

The output of `show pipelines` and the `/pipeline/list` API now includes the
start time of the pipeline in the field `start_time`, the newly added retry
delay in the field `retry_delay`, and whether the pipeline is hidden from the
overview page on app.tenzir.com in the field `hidden`.
