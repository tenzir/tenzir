### 🟡 P3 · 🧪 TST-1 · No test covers delimiter handling across chunk boundaries · 88%
- **File**: `libtenzir/builtins/operators/read_delimited.cpp:49-75`, `test/tests/operators/read_delimited/*.tql`
- **Issue**: The new operator keeps manual cross-chunk state (`buffer_` + `consumed`) but the staged tests only exercise single in-memory lines and do not validate chunk-split delimiters.
- **Reasoning**: `read_delimited` is a streaming operator over `chunk_ptr`; correctness depends on behavior when delimiters are split across chunk boundaries, which is exactly where regressions tend to hide.
- **Evidence**: The implementation has explicit boundary-sensitive logic (`break` when delimiter ends at buffer tail, then carry remainder into next `process()` call), but none of the added tests force multiple chunks or split a delimiter between chunks.
- **Suggestion**: Add at least one integration test that feeds data in multiple chunks with (1) a separator split across chunks and (2) consecutive separators spanning chunk boundaries.

### ~~🟡 P3 · 📖 DOC-1 · User-facing operator added without accompanying docs update~~ · N/A
- **Resolution**: Not needed. This is a re-implementation of an existing operator (`read_delimited`); docs already exist at `docs/src/content/docs/reference/operators/read_delimited.mdx`.

╭───────────────────────────────────────────╮
│ 🔴 P1: 0   🟠 P2: 0   🟡 P3: 1   ⚪ P4: 0 │
╰───────────────────────────────────────────╯
**Ship with fixes**:
- Add chunk-boundary integration coverage for `read_delimited` before merge (requires `buffer` to support blocking mode correctly first).
- Merge once TST-1 is addressed.
