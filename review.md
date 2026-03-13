# PR #5903 — Port summarize operator: Review Comments

**PR:** https://github.com/tenzir/tenzir/pull/5903  
**Reviewed commit:** `22a65f3b`  
**Automated reviewers:** Codex (chatgpt-codex-connector), Greptile

---

## P3 — Timer drift: sleep restarted after flush, not on wall-clock intervals

**File:** `libtenzir/builtins/operators/summarize.cpp:694–706`  
**Source:** Greptile

`await_task` restarts `sleep_for(*cfg_.frequency)` only after `process_task`
(and thus `flush()`) fully completes. The actual period between periodic flushes
is therefore `frequency + flush_duration` rather than a fixed wall-clock interval.

The legacy path uses `detail::weak_run_delayed_loop`, which fires on a fixed
wall-clock schedule regardless of flush duration:

```
Legacy:  |--freq--|--freq--|--freq--|       (fixed wall-clock)
New:     |--freq--|flush|--freq--|flush|   (drifting)
```

For typical use (e.g. `frequency: 1m`) this is negligible, but it is a silent
behavioral change. Consider documenting it in the class-level comment, or anchoring
the timer to wall-clock time as the legacy path does.

---

## P3 — Last periodic flush may be skipped when `EndOfData` races the timer

**File:** `libtenzir/builtins/operators/summarize.cpp:685–691`  
**Source:** Greptile

The legacy `summarize_operator2` path drains any pending flush before the final
`finish()`:

```cpp
if (std::exchange(pending_flush, false)) {
  for (auto result : impl.flush()) { co_yield result; }
}
for (auto result : impl.finish()) { co_yield result; }
```

The new `Summarize::finalize()` calls `impl_->finish()` directly with no such
drain. If `EndOfData` arrives in the executor queue before the timer's
`ExplicitAny`, `finalize()` is called while the timer message is still pending.
When that timer message is processed it is immediately skipped
(`if (is_done_ or got_shutdown_request_) { co_return; }`), so the last scheduled
periodic flush is silently dropped and its data is absorbed into the final
`finalize()` emission — which in `"reset"` mode produces different output than a
flush-then-final sequence.

**Fix:** Add a `pending_flush` flag analogous to the legacy path; check it in
`finalize()`.

---

## P1 — Implement serialization

**File:** `libtenzir/builtins/operators/summarize.cpp:716–728`  

---

## Summary

🟡 P3 · 💬 GIT-1 · Timer drift: sleep restarted after flush, not on wall-clock intervals · `summarize.cpp:694`
🟡 P3 · 💬 GIT-2 · Last periodic flush may be skipped when EndOfData races the timer · `summarize.cpp:685`
🔴 P1 · 💬 GIT-3 · Implement serialization · `summarize.cpp:716`

╭───────────────────────────────────────────╮
│ 🔴 P1: 1   🟠 P2: 0   🟡 P3: 2   ⚪ P4: 0 │
╰───────────────────────────────────────────╯
**Blocked**:
- Implement `snapshot()` serialization before shipping — the current no-op means aggregation state is silently lost across restarts.
- Address the timer-drift and last-flush-race issues together, as both stem from the `await_task`/`finalize` split; consider a `pending_flush` flag in `finalize()` and documenting the drift behavior.

