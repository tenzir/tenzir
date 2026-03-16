# Review: PR #5903 — Port summarize operator

**Branch:** `topic/summarize-compiler-plugin` → `main`  
**Author:** @aljazerzen

This PR ports the `summarize` operator to the new executor path by wiring it
into the compiler pipeline. The new path introduces a `Summarize` async
operator, a `summarize_ir` IR node, and migrates/expands integration tests.

---

## Findings

### ~~🔴 P1 · 💬 GIT-1 · Dangling pointer when robin_map rehashes during group iteration · 92% (@chatgpt-codex-connector)~~ ✅ Fixed

**File:** `libtenzir/builtins/operators/summarize.cpp:248`

**Issue:** `find_or_create_group` returns `&it.value()` and the raw pointer is
retained as `current_group` across loop iterations that keep inserting new
group keys.

**Reasoning:** With `tsl::robin_map`, `emplace_hint` may trigger a table
resize and relocate all existing elements. A subsequent `update_group(*current_group, ...)` or the pointer comparison `current_group != group`
then dereferences a dangling pointer, which is undefined behavior that can
silently corrupt summarization results or crash the process.

**Fix:** `groups_.reserve(groups_.size() + static_cast<size_t>(total_rows))`
is called before the first `find_or_create_group` invocation. This pre-allocates
enough buckets for up to `total_rows` new insertions, preventing any rehash
during the loop and keeping all returned `bucket2*` pointers stable.

---

### 🔴 P1 · 💬 GIT-2 · snapshot() guard silently skips restore because impl_ is null until start() · 91% (@coderabbitai)

**File:** `libtenzir/builtins/operators/summarize.cpp:824`

**Issue:** The `if (impl_)` guard in `snapshot()` prevents deserialization
because the executor calls `snapshot()` for restore *before* `start()`, where
`impl_` is created.

**Reasoning:** The executor lifecycle is: construct → `snapshot()` (restore) →
`start()`. Since `impl_` is constructed inside `start()`, it is `nullptr` when
the restore path calls `snapshot()`. The guard silently falls through, so all
checkpointed state — group keys, per-group aggregation values, `previous_values_`
for update mode — is dropped. The operator restarts from zero and produces
incorrect cumulative/update-mode results.

**Evidence:**
```cpp
auto snapshot(Serde& serde) -> void override {
    if (impl_) {           // impl_ is null during restore — body is never entered
        serde("state", *impl_);
    }
}
// impl_ is only created here, called after snapshot() on restore:
auto start(...) -> Task<void> override {
    impl_ = std::make_unique<implementation2>(...);
```

**Suggestion:** Either (a) eagerly construct `impl_` in the `Summarize`
constructor so it exists before `snapshot()` is called, or (b) deserialize into
a staging blob (`bucket2::blobs_` or a dedicated `std::optional<chunk_ptr>`)
in `snapshot()` without the guard, and drain that staging area inside `start()`.

---

### 🟠 P2 · 💬 GIT-3 · Build script hardcodes build path, breaks non-default configurations · 88% (@chatgpt-codex-connector)

**File:** `scripts/build.sh:10`

**Issue:** `build_dir` is hardcoded to `build/clang/release` (unless `BUILD_DIR`
is set), removing the prior logic that searched for any configured build
directory.

**Reasoning:** Developers using a different preset (e.g., `build/gcc/debug`,
`build/clang/debug`, a custom path) will get a hard error
`error: build directory '…' not found` when running `scripts/build.sh` without
explicitly setting `BUILD_DIR`. The script's own header comment says it
"auto-discovers the build directory", which is now false.

**Evidence:**
```bash
repo_root=$(cd "$(dirname "$0")/.." && pwd)   # unused after the change
build_dir="${BUILD_DIR:-build/clang/release}"  # single hardcoded default

if [[ ! -d "$build_dir" ]]; then
  echo "error: build directory '$build_dir' not found ..." >&2
  exit 1
fi
```

**Suggestion:** Restore discovery by searching for a `CMakeCache.txt` under
`build/` (e.g., `find build -maxdepth 3 -name CMakeCache.txt -print -quit`) and
using the containing directory, falling back to the hardcoded default only when
no configured build is found. Also remove the now-unused `repo_root` assignment
(see GIT-6 below).

---

### ⚪ P4 · 💬 GIT-4 · Silent discard of non-record value in emplace_value lacks trace log · 80% (@coderabbitai)

**File:** `libtenzir/builtins/operators/summarize.cpp:372`

**Issue:** When `sel.path().empty()` and the aggregation value is not a `record`,
the value is silently dropped with no diagnostic or log entry.

**Reasoning:** The behavior is intentional and documented in a comment, but if
an aggregation function accidentally returns a scalar where a record is expected,
the bug will be invisible — no log, no error, no observable artifact.

**Evidence:**
```cpp
if (sel.path().empty()) {
    if (auto* rec = try_as<record>(&value)) {
        root = std::move(*rec);
    }
    return;   // non-record value silently discarded
}
```

**Suggestion:** Add a `TENZIR_TRACE` (or at minimum `TENZIR_DEBUG`) log inside
the `else` branch to record the discarded value type and selector path, making
unexpected aggregation mismatches traceable in debug builds.

---

### ⚪ P4 · 💬 GIT-5 · Mode validation uses manual OR-chain; set-based lookup is more maintainable · 78% (@coderabbitai)

**File:** `libtenzir/builtins/operators/summarize.cpp:665`

**Issue:** The three valid mode strings are validated with a `||` chain that
would need a fourth branch every time a new mode is added.

**Evidence:**
```cpp
if (*str == "reset" || *str == "cumulative" || *str == "update") {
    cfg.mode = *str;
} else { ... }
```

**Suggestion:** Extract a `static const std::set<std::string_view>` of valid
modes and use `contains()`. This makes the valid-mode list easy to locate, and
keeps the validation expression O(log n) rather than O(n) in the number of
modes.

---

### ⚪ P4 · 💬 GIT-6 · repo_root assigned but never used in build.sh · 82% (@coderabbitai)

**File:** `scripts/build.sh:9`

**Issue:** `repo_root` is computed but never referenced after the auto-discovery
logic was removed.

**Evidence:**
```bash
repo_root=$(cd "$(dirname "$0")/.." && pwd)   # unused
build_dir="${BUILD_DIR:-build/clang/release}"
```

**Suggestion:** Remove the `repo_root` line entirely (or, if auto-discovery is
restored per GIT-3, repurpose it as the search root).

---

## Summary

```
╭───────────────────────────────────────────╮
│ 🔴 P1: 1   🟠 P2: 1   🟡 P3: 0   ⚪ P4: 3 │
╰───────────────────────────────────────────╯
```

**Needs changes:**
- Fix the snapshot restore path (GIT-2): move `impl_` construction before `snapshot()` is called, or deserialize into a staging area; otherwise checkpoint/restore silently produces wrong results.
- Restore build-directory auto-discovery in `scripts/build.sh` (GIT-3) and remove the dead `repo_root` variable (GIT-6).
- ~~GIT-1 resolved~~: `groups_.reserve()` before the group-iteration loop prevents robin_map rehash and pointer invalidation.
