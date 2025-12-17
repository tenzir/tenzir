---
title: "Deprecate builds without Apache Arrow"
type: change
author: dominiklohmann
created: 2021-05-26T14:07:52Z
pr: 1682
---

Building VAST without Apache Arrow via `-DVAST_ENABLE_ARROW=OFF` is now
deprecated, and support for the option will be removed in a future release. As
the Arrow ecosystem and libraries matured, we feel confident in making it a
required dependency and plan to build upon it more in the future.
