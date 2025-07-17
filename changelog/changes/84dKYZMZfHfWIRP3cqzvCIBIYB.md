---
title: "Fix shutdown hang in sources on SIGTERM/SIGINT"
type: bugfix
authors: dominiklohmann
pr: 1718
---

Import processes no longer hang on receiving SIGINT or SIGKILL. Instead, they
shut down properly after flushing yet to be processed data.
