A long-standing performance bug that caused query evaluation to look at more
candidate partitions than necessary no longer exists: A query for `ts`, e.g.,
for matching the field `zeek.conn.ts`, stops evaluating unrelated fields that
end in `ts`, e.g., `suricata.flow.flow.pkts` at an earlier point in the
pipeline. Similarly, a field exactly named `ts` of a type incompatible with the
query will no longer lead to the partition being considered.
