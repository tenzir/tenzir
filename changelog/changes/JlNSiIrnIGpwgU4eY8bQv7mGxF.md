---
title: "Make vast.conf lookup on Linux systems more intuitive"
type: change
authors: dominiklohmann
pr: 1036
---

The global VAST configuration now always resides in
`<sysconfdir>/vast/vast.conf`, and bundled schemas always in
`<datadir>/vast/schema/`. VAST no longer supports reading a `vast.conf` file in
the current working directory.
