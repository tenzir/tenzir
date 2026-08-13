---
title: Sigma matches become OCSF Detection Findings
type: change
authors:
  - mavam
prs:
  - 116
created: 2026-08-13T14:03:00.652766Z
---

The `sigma` operator now emits an OCSF 1.9.0 Detection Finding for every rule
match instead of the previous `tenzir.sigma` record with `event` and `rule`
fields.

For example, apply a rule and inspect the event, causal identifiers, matched
fields, and condition trace:

```tql
from {user: "alice", action: "login"}
sigma rules=r#"
title: Alice login
detection:
  selection:
    user: alice
  condition: selection
"#
select event = evidences[0].data,
       identifiers = finding_info.traits,
       fields = evidences[1].sigma.fields,
       trace = evidences[1].sigma.trace
```

Each finding includes:

- The complete applied rule in `policy.data` and its normalized identity in
  `finding_info.analytic`.
- The rule's severity in `severity_id` and MITRE ATT&CK tags in `attacks`.
- The matching event in `evidences[0].data`.
- Causal search identifiers in `finding_info.traits` and positive matched field
  values in `observables`.
- Detailed matcher, case, polarity, and condition-trace provenance in a second
  evidence entry at `evidences[1].sigma`.

To restore the previous output shape, append this projection:

```tql
this = {event: evidences[0].data, rule: policy.data}
```
