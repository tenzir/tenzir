---
title: Sigma rules for generic proxy, web server, and DNS logs on OCSF
type: feature
authors:
  - mavam
created: 2026-11-16T08:26:02.000000Z
---

The `sigma` operator now matches more stock Sigma rules against OCSF events.
Rules for Sigma's generic `proxy` and `webserver` categories match HTTP
Activity, and rules for the generic `dns` category match DNS Activity, from
any producer:

```yaml
title: Google Drive Direct Download
logsource:
  category: proxy
detection:
  selection:
    c-uri|contains|all:
      - drive.google.com
      - uc?id
      - export=download
  condition: selection
```

Proxy and web server rules read the URL, path, query, host, method, user
agent, referrer, status code, byte counts, and endpoint addresses from their
OCSF attributes. Fields without an OCSF counterpart, such as `c-uri-extension`
and `cs-cookie`, skip the rule with a diagnostic that names the field.
