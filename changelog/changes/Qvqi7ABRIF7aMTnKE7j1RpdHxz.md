---
title: "Improve `to_splunk` TLS functionality"
type: bugfix
authors: raxyte
pr: 4825
---

The `max_content_length` option for the `to_splunk` operator was named incorrectly in
an earlier version to `send_timeout`. This has now been fixed.
