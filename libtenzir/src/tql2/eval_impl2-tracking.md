# IMPORTANT

* Do not touch any files besides `libtenzir/src/tql2/eval_impl2.cpp`, `libtenzir/src/tql2/eval_unary2.cpp`, and `libtenzir/src/tql2/eval_impl2-tracking.md`
* If you have a question, ignore the problematic port and continue with the next. Mark the problems in this file.

# `eval_impl2` Port Tracker

Header: `libtenzir/include/tenzir/tql2/eval_impl2.hpp`

Sources: `libtenzir/src/tql2/eval_impl2.cpp`, `libtenzir/src/tql2/eval_unary2.cpp`

Primary inspiration sources: `libtenzir/src/tql2/eval_impl.cpp`, `libtenzir/src/tql2/eval_unary.cpp`

## Not Yet Implemented

| Function | Current state | Porting note | Header | Source | Inspiration |
| --- | --- | --- | --- | --- | --- |
| `tenzir2::evaluator::to_array(tenzir2::data const&)` | Stub | `eval(tenzir::ast::constant const&, ...)` now has a private legacy-data bridge via `append_legacy_data` and `make_legacy_constant_array`, but `to_array()` itself still returns `make_null_array(length_)`. This stays blocked in the current slice because `tenzir2::data` has no public variant-traits access, and the current `value_view_<record>` / `value_view_<list>` accessor surface is not complete enough to build a safe generic converter locally in `eval_impl2.cpp`. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp` |
| `tenzir2::evaluator::eval(tenzir::ast::function_call const&, tenzir::ActiveRows const&)` | Not implemented | Intentionally deferred in this step. The remaining work depends on the legacy TQL function/plugin evaluator and bridge code outside `eval_impl2.cpp`, which still speaks `multi_series`. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp` |
| `tenzir2::evaluator::eval(tenzir::ast::binary_expr const&, tenzir::ActiveRows const&)` | Not implemented | Needs a `tenzir2` port of `eval_binary.cpp`, including short-circuiting and active-row propagation without `multi_series`. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp`, `libtenzir/src/tql2/eval_binary.cpp` |
| `tenzir2::evaluator::eval(tenzir::ast::lambda_expr const&, array_<list> const&)` | Not implemented | Intentionally deferred in this step. `eval2.hpp` already declares the free-function overload that also takes a `TableSlice`, but the implementation work lives outside `eval_impl2.cpp` under the broader `eval.cpp` port, and `eval_impl2.cpp` still returns `not_implemented(x)`. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp` |

## Partial Ports

| Function | Current state | Remaining gap | Header | Source | Inspiration |
| --- | --- | --- | --- | --- | --- |
| `tenzir2::evaluator::eval(tenzir::ast::constant const&, tenzir::ActiveRows const&)` | Partial | Supports `null`, booleans, signed and unsigned integers, doubles, duration, time, strings, IPs, subnets, empty lists, and records via `append_legacy_data`. `pattern`, `enumeration`, non-empty legacy lists, `map`, `blob`, and `secret` are still unsupported, so evaluation warns and falls back to null. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp` |
| `tenzir2::evaluator::eval(tenzir::ast::field_access const&, tenzir::ActiveRows const&)` | Partial | Direct projection and row-wise record access are ported, including legacy-shaped null/non-record/missing-field warnings and field-name suggestions on uniform record arrays. Remaining differences are mainly exact batching and inactive-row behavior versus the legacy `multi_series` implementation. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp` |
| `tenzir2::evaluator::eval(tenzir::ast::index_expr const&, tenzir::ActiveRows const&)` | Partial | Constant string index access (`foo["bar"]`) is still forwarded to field access, and row-wise string/int indexing now covers records and lists, including negative list indices, split non-record versus record/list diagnostics, list-null wording, and suggestions for missing fields on uniform record arrays. Remaining gaps are mostly exact batching behavior and edge-case diagnostic parity versus the legacy `multi_series` implementation. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp` |
| `tenzir2::evaluator::eval(tenzir::ast::unary_expr const&, tenzir::ActiveRows const&)` | Partial | Ported in `eval_unary2.cpp` for `+`, `move`, `not bool`, `-int64`, `-uint64`, `-double`, and `-duration`, with legacy-shaped unsupported-type diagnostics. The main remaining gap is mixed union-array active-row parity: `access::transform` only exposes typed subarrays, not their original row positions, so both supported-op inactive-row masking and unsupported-type warning gating still diverge from legacy `multi_series` behavior in those cases. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_unary2.cpp` | `libtenzir/src/tql2/eval_impl.cpp`, `libtenzir/src/tql2/eval_unary.cpp` |
| `tenzir2::evaluator::eval(tenzir::ast::format_expr const&, tenzir::ActiveRows const&)` | Partial | Ported to always produce a single `array_<std::string>` using `tenzir2::format_tql`. Legacy secret-preserving output splitting is intentionally not mirrored yet because `tenzir2` currently has no `secret` data type. | `libtenzir/include/tenzir/tql2/eval_impl2.hpp` | `libtenzir/src/tql2/eval_impl2.cpp` | `libtenzir/src/tql2/eval_impl.cpp` |
