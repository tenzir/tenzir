---
title: "Reduce conflict potential between rebuilding and queries"
type: change
authors: dominiklohmann
pr: 3047
---

VAST's rebuilding and compaction features now interfere less with queries. This
patch was also backported as [VAST v2.4.2](https://vast.io/changelog#v242) to
enable a smoother upgrade from to VAST v3.x.
