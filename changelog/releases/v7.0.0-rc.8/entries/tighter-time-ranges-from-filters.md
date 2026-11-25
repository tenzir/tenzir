---
title: Tighter time ranges from filters
type: change
authors:
  - mavam
created: 2026-11-15T22:08:05.28601Z
---

Sources that narrow the time range of their requests to the filters after them now share one reading of those filters. A strict bound now excludes its time, so `from_splunk` requests `latest_time=1735689720` instead of `1735689721` for `where _time < 2025-01-01T00:02:00Z`. `from_microsoft_365_activity` now also narrows the listing to the range that encloses an `or`, such as for `where microsoft_365_activity.content_created >= 2024-01-02 or microsoft_365_activity.content_created == 2024-01-01`, and narrows the side of an `==` that it can represent.
