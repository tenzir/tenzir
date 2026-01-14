---
title: "Update the repository to include retry delay-related bug fixes"
type: bugfix
author: Dakostu
created: 2024-05-10T11:35:35Z
pr: 4184
---

Some pipelines did not restart on failure. The retry mechanism now works for all
kinds of failures.

Pipelines that are configured to automatically restart on failure can now be
stopped explicitly. Stopping a failed pipeline now always changes its state to
the stopped state.
