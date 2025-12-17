---
title: "Add Zeek writer plugin"
type: feature
author: mavam
created: 2021-08-27T11:28:32Z
pr: 1863
---

The `broker` plugin is now a also *writer* plugin on top of being already a
*reader* plugin. The new plugin enables exporting query results directly into a
a Zeek process, e.g., to write Zeek scripts that incorporate context from the
past. Run `vast export broker <expr>` to ship events via Broker that Zeek
dispatches under the event `VAST::data(layout: string, data: any)`.
