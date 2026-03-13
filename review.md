# PR #5903 — Port summarize operator: Review Comments

**PR:** https://github.com/tenzir/tenzir/pull/5903  
**Reviewed commit:** `22a65f3b`  
**Automated reviewers:** Codex (chatgpt-codex-connector), Greptile

---

## P1 — Implement serialization

**File:** `libtenzir/builtins/operators/summarize.cpp:716–728`  

---

## Summary

🔴 P1 · 💬 GIT-1 · Implement serialization · `summarize.cpp:716`

╭───────────────────────────────────────────╮
│ 🔴 P1: 1   🟠 P2: 0   🟡 P3: 0   ⚪ P4: 0 │
╰───────────────────────────────────────────╯
**Blocked**:
- Implement `snapshot()` serialization before shipping — the current no-op means aggregation state is silently lost across restarts.

