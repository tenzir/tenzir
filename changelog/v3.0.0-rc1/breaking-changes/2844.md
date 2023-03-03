Boolean literals in expressions have a new syntax: `true` and `false` replace
the old representations `T` and `F`. For example, the query
`suricata.alert.alerted == T` is no longer valid; use `suricata.alert.alerted ==
true` instead.
