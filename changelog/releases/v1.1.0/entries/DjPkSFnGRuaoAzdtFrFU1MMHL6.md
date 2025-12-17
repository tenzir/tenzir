---
title: "Log actor names together with the unique actor ID"
type: change
author: tobim
created: 2022-02-28T08:08:31Z
pr: 2119
---

Actor names in log messages now have an `-ID` suffix to make it easier to tell
multiple instances of the same actor apart, e.g., `exporter-42`.
