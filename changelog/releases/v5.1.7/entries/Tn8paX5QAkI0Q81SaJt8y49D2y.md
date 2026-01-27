---
title: "Allow packages to be disabled properly"
type: bugfix
author: dominiklohmann
created: 2025-05-19T09:34:08Z
pr: 5161
---

For configured pipelines, the `tenzir.pipelines.<pipeline>.disabled`
configuration option was silently ignored unless the pipeline was part of a
package. This no longer happens, and disabling the pipelines through the option
now works correctly.
