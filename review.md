# PR Review: Port charts operators (#5869)

This PR ports the `chart_area`, `chart_bar`, `chart_line`, and `chart_pie`
operators from the old legacy plugin architecture to the new
`OperatorPlugin`/`Describer` API with TQL2. It's a substantial and
well-structured rewrite. Here are my observations:

---

## ✅ What's done well

- **Clean architecture**: The new design uses a single `Chart<Ty>` template
  class with a `ChartArgs<Ty>` struct, eliminating a lot of brittle
  string-based attribute manipulation from the old code.
- **Proper aggregation support**: The new code uses actual
  `aggregation_instance` objects (`Bucket`, `GroupedBucket`) for computing `y`
  values with proper incremental `update()`/`get()` calls, replacing the old
  pass-through that just forwarded rows with type attributes.
- **Multi-alias named args** (`operator_plugin.hpp`): The `Named` struct now
  supports multiple names (e.g. `{"x", "label"}` and `{"y", "value"}` for
  bar/pie) — a nice, minimal, backward-compatible extension.
- **Gap filling**: The `find_gap`/`fill_at` logic for `resolution`-based time
  series is clean and correct.
- **Good test coverage**: New tests for `bucketing`, `grouping`, `limit`,
  `pie_spread_error`, `range_mixed`, `range_mixed2`, `range_mixed3`. Legacy
  tests migrated to the new test directory.

---

## 🐛 Bugs / Correctness Issues

### 0. validate_chart_common and the range check

libtenzir/builtins/operators/chart.cpp:1003
In validate_chart_common, mixed numeric limits are type-normalized with to_double, but the range check still compares the original variants via ymin->inner >= ymax->inner. tenzir::data compares different alternatives by variant index, so cases like y_min=100 and y_max=2.5 can incorrectly pass (and some valid mixes can fail) even though the intended numeric ordering is wrong. Since prepare later coerces both limits to doubles, this allows invalid y-axis bounds to reach execution instead of being


### 1. `consumed` not incremented when `get_bucket` returns null in the per-row loop

In `process_slice`:

```cpp
auto [newb, new_bucket] = get_bucket(groups_, x, group_name, ctx);
if (b != newb or new_bucket) {
    if (b) { /* flush b */ }
    ...
    consumed += i;
    i = 0;
}
++i;
```

If `get_bucket` returns `{nullptr, false}` for a row (e.g. null x-value, or
x-value that hit the limit), the loop keeps incrementing `i` and will later
call `subslice(slice, consumed, consumed + i)` past those skipped rows, feeding
them into the wrong bucket's `update()`. The null/limit check happens inside
`get_bucket` but the aggregation boundary tracking (`consumed`) doesn't account
for those skipped rows.

### 2. `find_gap` mutates `prev` as a side effect

```cpp
auto find_gap(std::optional<data>& prev, data const& curr) const
  -> std::optional<data>;
```

This function both returns a gap value *and* initialises `prev = curr` on first
call (when `prev` is nullopt). A function named `find_gap` shouldn't have the
side effect of initialising `prev`. It works because callers pass local
variables, but the dual role makes the gap-filling loops in `build_output` hard
to follow. Consider splitting into an explicit init step and a pure gap query.

### 3. `x_min` fill boundary loop is fragile

In `build_output`, filling before the first data point:

```cpp
auto min = std::optional{prep_->x_min->rounded};
auto const& first = groups_.begin()->first;
if (*min != first) {
    fill_at(*min);
}
while (auto gap = find_gap(min, first)) {
    min = gap.value();
    fill_at(std::move(gap).value());
}
```

The `find_gap` call here also has the side effect of setting `min = first`
when the while-loop terminates (because `find_gap` sets `prev = curr`). This
interacts with the mutable `prev` semantics noted above and is easy to break.
An explicit arithmetic loop would be clearer.

---

## ⚠️ Design / Code Quality Issues

### 4. `validate_chart_common` uses an accidental `if constexpr` type hack for position

```cpp
if constexpr (requires {
                pos->inner;
                typename decltype(pos->inner)::value_type;
              }) {
  // It's a string
  if (pos->inner != "stacked" && pos->inner != "grouped") { ...
```

The intent is to distinguish `located<std::string>` (area/bar) from
`located<uint64_t>` (the dummy `limit` passed by `PluginLine`). The check
works today because `uint64_t` has no `value_type`, but it's fragile — any
other type with a `value_type` would pass through. Better to factor out a
dedicated `validate_position()` free function and not call it for `PluginLine`
at all.

### 5. `PluginLine` passes `limit` as a dummy for `position`

```cpp
// Line chart doesn't have position, so pass limit as a dummy (ctx.get will
// return nullopt)
return validate_chart_common(y, limit, limit, x_min, x_max, ...);
```

`ctx.get(limit)` returns the *actual* limit value (not nullopt), so the
position-validation branch could fire unexpectedly if the `if constexpr` type
guard above ever changes. Better to give `validate_chart_common` an explicit
`bool has_position` parameter, or factor `validate_position()` out of the
common function.

### 6. `xpath` is computed twice

The same loop that joins `args_.x.path()` segments into an `xpath` string
appears in both `process_slice` (lines 337–340) and `build_output` (lines
420–423). This should be a private helper or computed once during `prepare()`
and cached alongside `Prepared`.

### 8. `make_attributes` mutates `ynums` as a side effect

```cpp
for (auto i = ynums.size(); i < prep_->y.size() or i < ynames.size(); ++i) {
    ynums.emplace_back(fmt::format("y{}", i));
}
```

Growing `ynums` inside what looks like a read-only "make" function is
surprising. Since `ynums` is a local `std::deque` in `build_output` this works,
but the mutation should either happen before the call or the function should
take full ownership of constructing the list.


---

## 🔍 Minor issues

- `summarize.cpp`: stray blank-line removal (cosmetic, unrelated to the PR).
- `chart2.cpp` fix (`auto success = printer.print(...)` instead of asserting
  the assignment expression directly) is a good cleanup.
- `operator_plugin.cpp`: `primary_name()` is only called in one place and could
  be inlined; `display_names()` could use `fmt::join` directly at the call
  site. Both are fine as named helpers.
- `fill_and_group.txt`: field ordering changed from `b_*` before `a_*` to
  `a_*` before `b_*`. The new order (reflecting stable insertion order of
  groups as seen in the input) is correct and consistent.
- In `handle_xlimit`, the `duration` branch correctly handles negative durations
  with `std::abs` when snapping to the resolution grid — good edge-case
  coverage.
