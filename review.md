# Review — commit `aa13939` ("serialize")

**Date:** 2026-03-13  
**Author:** Aljaž M. Eržen  
**Scope:** Implements `snapshot()` serialization for the `summarize` operator; removes `session ctx` from `aggregation_instance::restore()`; replaces user-facing `diagnostic::warning` calls with `TENZIR_WARN` in all aggregation restore paths.

---

## Overview

The commit delivers a long-pending feature: aggregation state in `summarize` now survives restarts via the snapshot/restore mechanism. The approach is sound — a two-phase restore (`bucket2::inspect` stages raw blobs; `implementation2::inspect`'s `on_load` callback re-inflates them via `make_bucket()` + `restore()`) and the removal of `unique_ptr` indirection is a clean simplification. The changes to `restore()` signature and diagnostics are applied consistently across all aggregation functions.

Three issues warrant attention before this is considered fully production-safe.

---

## Findings

### 🔴 P1 · 🏗️ ARC-1 · Hard abort on aggregation-count mismatch after config change · 95%

- **File:** `libtenzir/builtins/operators/summarize.cpp` — `implementation2::inspect` `on_load` lambda
- **Issue:** `TENZIR_ASSERT(bucket.blobs_.size() == fresh.aggregations.size())` will terminate the process if a snapshot is restored against a pipeline definition that has a different number of aggregate functions.
- **Reasoning:** Users commonly edit pipelines — adding or removing an aggregation column is a routine change. When the operator resumes after a restart with an existing snapshot and the pipeline now has a different number of aggregates, this assert fires and takes down the node. The snapshot mechanism exists precisely to survive such operational events, so a hard abort defeats the purpose.
- **Evidence:**
  ```cpp
  auto on_load = [&]() noexcept {
    for (auto it = x.groups_.begin(); it != x.groups_.end(); ++it) {
      auto& bucket = it.value();
      auto fresh = x.make_bucket();
      TENZIR_ASSERT(bucket.blobs_.size() == fresh.aggregations.size()); // <-- terminates
      for (auto i = size_t{0}; i < bucket.blobs_.size(); ++i)
        fresh.aggregations[i]->restore(std::move(bucket.blobs_[i]));
      ...
    }
  };
  ```
- **Suggestion:** Replace the assert with a graceful recovery: log a warning and discard the stale snapshot (reset to a fresh `groups_` map) rather than crashing. A sentinel version field in the serialized state could make this even more robust in future.

---

### 🟠 P2 · 👁️ RDY-1 · `noexcept` lambda calls functions that can throw · 90%

- **File:** `libtenzir/builtins/operators/summarize.cpp` — `implementation2::inspect`
- **Issue:** The `on_load` lambda is declared `noexcept` but calls `make_bucket()`, which calls `.unwrap()` on a `failure_or`, and `restore()` implementations — either can throw.
- **Reasoning:** If any of those calls throw, the `noexcept` contract causes `std::terminate`. Since `make_bucket()` asserts inside and `.unwrap()` panics on failure, this is latent undefined behaviour under any error path.
- **Evidence:**
  ```cpp
  auto on_load = [&]() noexcept {          // <-- noexcept
    auto fresh = x.make_bucket();          // calls .unwrap() internally
    fresh.aggregations[i]->restore(...);   // implementations can TENZIR_WARN + return but could also throw
    ...
  };
  ```
- **Suggestion:** Either remove `noexcept` (if the CAF inspector framework permits it), or guard `make_bucket()` with an explicit non-throwing path and document the invariant clearly.

---

### 🟡 P3 · 🎨 UXD-1 · Restore failures silently corrupt aggregation results · 88%

- **File:** All `aggregation-functions/*.cpp` — `restore()` implementations
- **Issue:** All restore-error paths were downgraded from `diagnostic::warning` (user-visible) to `TENZIR_WARN` (server-log only). A corrupt or mismatched snapshot now silently resets the aggregation to a zero/empty state with no feedback to the pipeline author.
- **Reasoning:** The previous `diagnostic::warning` surfaced the error through the normal Tenzir diagnostics channel — the user could see that something went wrong. With only a server log, a pipeline author has no way to know that their `summarize` is computing from a blank slate rather than from the accumulated state they expect. This is especially dangerous for long-window aggregations.
- **Evidence:** Representative before/after from `collect.cpp`:
  ```cpp
  // Before
  diagnostic::warning("invalid FlatBuffer")
    .note("failed to restore `collect` aggregation instance")
    .emit(ctx);
  // After
  TENZIR_WARN("failed to restore `collect` aggregation instance: invalid FlatBuffer");
  ```
- **Suggestion:** Consider emitting at least one pipeline-level diagnostic when *any* restore in a bucket fails — e.g., at the `implementation2` level in `on_load` — so users see a visible warning that state was lost and aggregations have been reset.

---

### 🟡 P3 · 🎨 UXD-2 · `quantile` silently drops state in active snapshots · 90%

- **File:** `libtenzir/builtins/functions/numeric.cpp` — `quantile_aggregation_instance::restore`
- **Issue:** `quantile`'s `restore()` is still a no-op. Now that `summarize` snapshotting is active, any pipeline using `quantile(…)` inside `summarize` will silently reset its quantile state on every restart — there is no way for the user to know.
- **Reasoning:** Before this commit, snapshots were never written or read for `summarize`, so the missing `quantile` restore was harmless. Now it is a live regression for that aggregation function.
- **Evidence:**
  ```cpp
  auto restore(chunk_ptr chunk) -> void override {
    TENZIR_UNUSED(chunk);
    TENZIR_WARN("restoring `quantile` aggregation instance from snapshot is not yet implemented");
  }
  ```
  The warning goes to server logs only (see UXD-1 above), so users will not notice.
- **Suggestion:** Either implement `quantile` serialization (the t-digest structure is serializable), or — at minimum — make `quantile`'s `save()` return a null/empty chunk that `implementation2::inspect` can detect and skip, and emit a pipeline-level diagnostic so users know quantile state is ephemeral.

---

## Lower-confidence observations

- **`bucket2::blobs_` field name** — the trailing underscore is a member-variable naming convention in this codebase, but `blobs_` is a staging field that is empty during normal operation. A comment explaining its lifecycle (populated only during deserialization, cleared immediately in `on_load`) would help future readers. (~72%)

---

## Summary

```
╭───────────────────────────────────────────╮
│ 🔴 P1: 1   🟠 P2: 1   🟡 P3: 2   ⚪ P4: 0 │
╰───────────────────────────────────────────╯
```

**Needs changes**:
- Replace the `TENZIR_ASSERT` in `on_load` with graceful recovery — a config-change-triggered restart must not abort the node (ARC-1).
- Remove or fix the `noexcept` on `on_load` to eliminate the silent `std::terminate` risk under error conditions (RDY-1).
- Address the silent state-loss for `quantile` now that snapshots are live, and consider restoring at least a minimal pipeline-level diagnostic for any restore failure (UXD-1, UXD-2).
