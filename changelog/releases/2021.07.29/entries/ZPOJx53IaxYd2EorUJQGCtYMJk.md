---
title: "Make the source shutdown instantaneous"
type: bugfix
author: dominiklohmann
created: 2021-07-14T14:46:00Z
pr: 1771
---

Import processes now respond quicker. Shutdown requests are no longer delayed
when the server process has busy imports, and metrics reports are now written
in a timely manner.

Particularly busy imports caused the shutdown of the server process to hang,
if import processes were still running or had not yet flushed all data.
The server now shuts down correctly in these cases.
