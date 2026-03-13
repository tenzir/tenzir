# PR #5903 — Port summarize operator: Review Comments

**PR:** https://github.com/tenzir/tenzir/pull/5903  
**Reviewed commit:** `22a65f3b`  
**Automated reviewers:** Codex (chatgpt-codex-connector), Greptile

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

🟡 P3 · 💬 GIT-1 · Last periodic flush may be skipped when EndOfData races the timer · `summarize.cpp:685`
🔴 P1 · 💬 GIT-2 · Implement serialization · `summarize.cpp:716`

╭───────────────────────────────────────────╮
│ 🔴 P1: 1   🟠 P2: 0   🟡 P3: 1   ⚪ P4: 0 │
╰───────────────────────────────────────────╯
**Blocked**:
- Implement `snapshot()` serialization before shipping — the current no-op means aggregation state is silently lost across restarts.
- Fix the EndOfData/timer race by tracking pending flushes in finalize(), ensuring reset mode emits the last scheduled flush before finish.

