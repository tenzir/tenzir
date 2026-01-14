---
title: "Remove type layering"
type: bugfix
author: dominiklohmann
created: 2025-02-19T20:12:10Z
pr: 5008
---

We fixed a bug that caused `type_id(this)` to return inconsistent values for
schemas with metadata attached, e.g., after assigning a schema name via `@name =
"new_name"` or using operators like `chart_line` that add custom metadata to a
schema for use of the Tenzir Platform. Unfortunately, this may cause charts or
tables added to dashboards before Tenzir Platform v1.7 to break. To fix them,
click on the action menu on the chart or table on the dashboard, click "Open in
Explorer," and re-add the chart or table to the dashboard. We are sorry about
this inconvenience.
