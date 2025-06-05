---
title: "Upstream Debian patches"
type: change
authors: dominiklohmann
pr: 1515
---

We upstreamed the Debian patches provided by [@satta](https://github.com/satta).
VAST now prefers an installed `tsl-robin-map>=0.6.2` to the bundled one unless
configured with `--with-bundled-robin-map`, and we provide a manpage for
`lsvast` if `pandoc` is installed.
