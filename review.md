# PR #5986 — Unresolved review comments

Source: https://github.com/tenzir/tenzir/pull/5986

## Resolved in this branch

### ✅ GIT-1 · Reuse common helper via shared header

- Extracted `eval_as` into `libtenzir/include/tenzir/detail/eval_as.hpp`.
- Updated `libtenzir/builtins/operators/write_syslog.cpp` to call `detail::eval_as<...>`.
- Removed the in-class duplicated helper implementation from `WriteSyslog`.

### ✅ GIT-2 · Terminal parse path should use `failure_or<void>`

- Updated `process_octet(...)` in `libtenzir/builtins/operators/read_syslog.cpp` to return `Task<failure_or<void>>` instead of `Task<bool>`.
- Replaced `true`/`false` error signaling with explicit `failure::promise()` / success return.
- Updated call site in `process(...)` to handle `failure_or<void>` (`if (not result)`) for terminal error handling.

## Remaining unresolved comments

### 🟡 P3 · 💬 GIT-3 · Simplify duplicate builder-flush logic · 88%

- **File:** `libtenzir/builtins/operators/read_syslog.cpp:193`
- **Issue:** `flush_builders` and `finalize_builders` appear near-duplicate.
- **Reasoning:** Parallel implementations for similar behavior can diverge and make lifecycle semantics harder to reason about.
- **Evidence:** Reviewer @IyeOnline: “`flush_builders` and `finalize_builders` seem to be almost the same function. I think just `finalize_builders` should be sufficient.” (https://github.com/tenzir/tenzir/pull/5986#discussion_r3079847939)
- **Suggestion:** Consolidate to one code path (or factor shared internals), keeping snapshot/finalize semantics explicit.

### 🟡 P3 · 💬 GIT-4 · Prefer operator-location diagnostics over message prefixing · 86%

- **File:** `libtenzir/builtins/operators/read_syslog.cpp:153`
- **Issue:** Diagnostics are wrapped with a `"syslog parser"` prefix instead of attaching operator location metadata.
- **Reasoning:** Location-based diagnostics are more consistent with Tenzir conventions and improve downstream tooling/readability.
- **Evidence:** Reviewer @IyeOnline: “...we should simply add the operators location to it, rather than prepending ‘syslog parser’ to the message.” (https://github.com/tenzir/tenzir/pull/5986#discussion_r3079568033)
- **Suggestion:** Rework the wrapper to carry operator location in diagnostics and remove string-prefix augmentation.

### ⚪ P4 · 💬 GIT-5 · Avoid internal caching in `make_dh` helper · 83%

- **File:** `libtenzir/builtins/operators/read_syslog.cpp:151`
- **Issue:** `make_dh` owns/stores transformed handler state rather than returning a transformed handler for caller-managed storage.
- **Reasoning:** Caller-managed ownership keeps lifecycle/aliasing clearer and makes utility behavior more explicit.
- **Evidence:** Reviewer @IyeOnline: “...prefer it if this would just return the transforming handler and the caller were responsible for storing the result.” (https://github.com/tenzir/tenzir/pull/5986#discussion_r3079574755)
- **Suggestion:** Refactor helper API to return a transformed handler object/reference and keep storage at the call site.

### ⚪ P4 · 💬 GIT-6 · Address unresolved suggestion at include section · 74%

- **File:** `libtenzir/builtins/operators/read_syslog.cpp:39`
- **Issue:** An unresolved GitHub suggestion thread remains open near the includes.
- **Reasoning:** Even tiny unresolved suggestions can block merge if they reflect reviewer intent not yet acknowledged.
- **Evidence:** Reviewer @IyeOnline left an unresolved suggestion thread with an empty suggestion block (https://github.com/tenzir/tenzir/pull/5986#discussion_r3079575773).
- **Suggestion:** Clarify reviewer intent in-thread, apply/update the intended include change, then resolve.

## Lower-confidence observations

- The include-section suggestion thread (`GIT-6`) has minimal content, so intended change detail is unclear without a follow-up reply.

╭───────────────────────────────────────────╮
│ 🔴 P1: 0   🟠 P2: 0   🟡 P3: 2   ⚪ P4: 2 │
╰───────────────────────────────────────────╯
**Ship with fixes**:
- Address the two remaining P3 reviewer requests before merge, focusing on builder finalization duplication and diagnostic-location handling.
- Clarify and close the ambiguous include suggestion thread in-reply.
- Resolve remaining P4 cleanup items, then re-request review.
