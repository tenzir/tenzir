---
title: Windows Event Forwarding without a Windows collector
type: feature
authors:
  - mavam
created: 2026-10-02T18:49:05.994794Z
---

The new experimental [`accept_wef`](https://tenzir.com/docs/reference/operators/accept_wef) operator acts as a Windows Event Collector for source-initiated Windows Event Forwarding (WEF). Windows hosts forward events to Tenzir directly, so you no longer need a Windows Event Collector server or an agent on every host.

Hosts in an Active Directory domain authenticate with Kerberos. Point the subscription manager in your Group Policy at Tenzir, then define the subscriptions that the hosts should pick up:

```tql
let $logons = r#"
<QueryList>
  <Query Id="0">
    <Select Path="Security">*[System[(EventID=4624 or EventID=4625)]]</Select>
  </Query>
</QueryList>
"#
accept_wef kerberos={keytab: "/etc/tenzir/wef.keytab"},
  subscriptions=[{id: "security-logons", query: $logons}]
event = data.parse_winlog()
```

Hosts outside of a domain authenticate with client certificates through the `tls` option instead.

Each output event contains the raw event XML in `data`, the peer address, and the authenticated client, its claimed machine name, and the subscription in `wef`.

The operator stores where each client left off in the state directory, so clients resume after a restart without gaps. Subscriptions can target specific clients with `clients: {only: [...]}` or `clients: {except: [...]}`, and specific subscription manager URLs with `uri`. Clients may compress their batches.
