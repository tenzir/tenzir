# PR #5903 — Port summarize operator: Review Comments

**PR:** https://github.com/tenzir/tenzir/pull/5903  
**Reviewed commit:** `22a65f3b`  
**Automated reviewers:** Codex (chatgpt-codex-connector), Greptile

---

## P2 — Defer aggregation validation until after let substitution

**File:** `libtenzir/builtins/operators/summarize.cpp:497`  
**Source:** Codex

In the IR path, `compile()` builds the config before `summarize_ir::substitute()`
runs, but `add_aggregate` eagerly calls `make_aggregation` to validate arguments.
This rejects valid let-parameterized aggregations (e.g. `quantile(x, q=$q)`)
because `$q` is still unresolved at compile time, even though the call would be
valid after substitution.

**Fix:** Defer argument validation until substitution/spawn time.

---

## P2 — Evaluate summarize options after let substitution

**File:** `libtenzir/builtins/operators/summarize.cpp:437`  
**Source:** Codex

`build_config` const-evaluates `options.frequency` / `options.mode` immediately
during `compile()`, before let bindings are substituted into the IR operator.
Queries like `let freq = 1s; ... | summarize ..., options={frequency: $freq}`
therefore fail at compile time despite having valid bindings at instantiation time.

**Fix:** Defer option evaluation until substitution/spawn.

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

