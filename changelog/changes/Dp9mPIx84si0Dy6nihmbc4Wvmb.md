---
title: "Render duration and port as JSON strings"
type: change
authors: dominiklohmann
pr: 1034
---

The JSON export format now renders `duration` and `port` fields using strings as
opposed to numbers. This avoids a possible loss of information and enables users
to re-use the output in follow-up queries directly.
