# Nova Data Model

Nova is a custom columnar data model, independent of Arrow. Two conventions run
through it: copy-on-write via an `as_unique() const& / &&` pair, where the
rvalue overload reuses an exclusively owned buffer, and `storage::BitMap` masks
over a shared, uncompacted column instead of copies.

`storage::Index` is `std::int32_t`, capping arrays at ~2^31 rows. `-1` is a
pervasive sentinel: unset offset, absent record row, unassigned shape.

"Nova" is a temporary name; the model becomes the unnamed default. Do not
mention it in comments, docs, diagnostics, tests, or changelog entries. Existing
identifiers such as `nova::` and `--nova` stay.

## Types

### Registry

Types are compile-time tags, registered by membership in
`fundamental_type_list` and `structured_type_list`, whose join is
`data_type_list`. The concepts `fundamental_type`, `structured_type`, and
`data_type` test that membership; `concrete_or_erased_type` also admits the
erased `Data`. There are no type ids or runtime type handles — a type costs
zero bytes, and runtime type information lives in the array's variant tag.

`Null` is a fundamental type, not just a mask state, so nullness can be
structural. Union is in neither list: it is not a type, only an array
representation. This also means that there are no typed-nulls in this model.

### Storage Representation

`Type<Tag>` describes a tag. All specializations provide `static_name` and
`data_type`, checked by `fully_implemented_common`. Fundamental types add four
members, constrained by `fully_implemented_fundamental`: an owning `DataType`,
a cheap `ViewType` it converts to, a `PhysicalStorage` list of permitted
layouts, and a `PrimaryPhysicalStorage` that builders finish to. The concept
requires a non-empty list whose entries satisfy `storage::is_storage_api` and
expose a `ViewType` convertible to the logical one, with the primary among
them. `fully_implemented_type` combines both, demanding the fundamental half
only for non-structured tags.

One logical type thus maps to many layouts — narrow integers, `float`,
constants — which is where the memory wins are. Accessors widen to `ViewType`,
so it stays invisible logically.

## Arrays

### Fundamental

`Array<Tag>` is a variant over the tag's permitted physical storages; accessors
`match` and widen to `ViewType`. Validity is not in the array but composed:
`MaskedArray<A>{data, present}`, which record fields and union alternatives are
made of. **Absent** (`present == false`, via `none()`) differs from **null** (a
real `Null` value, via `null()`); reading an absent row with `get()` asserts.

### BitMap

BitMap is both the bitmap type used for masking as well as the physical
representation of bool column.

`storage::BitMap` packs into 128-bit words, caches `true_count_` so `any()` is
O(1), and represents all-true and all-false without allocating, so `&`, `|`,
`^`, and `and_not` short-circuit on constant masks. Use the bitmap iteration
utilities to efficiently iterate bitmaps rather than going through indices.
Use `keep_first(count)` to limit active rows without compacting the column.

### List

Offsets plus a flat `Array<Data>` child, behind a `shared_ptr` so copies are
cheap. The offsets buffer holds *N* entries, not Arrow's *N+1*: entry `i` is row
`i`'s end, the start is entry `i-1` or `0`.

`RowView<List>` is nova's only slice view. There is no general `Array::slice`.

### Record

A struct-of-columns with **per-row shapes**, so one array holds records with
differing field sets and orders. One `MaskedArray<Array<Data>>` column per
field name ever seen, a `names` map to column indices, and a `shape_indices`
column whose per-row `ShapeTable::ShapeId` names which fields that row has, in
which order; `-1` means the record is absent. A field absent within a present
shape reads back as null.

`ShapeTable` interns shapes as a DAG whose nodes memoize add/remove
transitions, so `record.foo = …` over a homogeneous batch is a cached hop, not a
rehash. Shape `0` is empty. `with_fields` and `without_fields` rewrite shapes
per *source shape* rather than per row.

### Union

`UnionArray` is a tag column over **full-length** masked alternatives: each
alternative spans every row, `present` marks the rows belonging to it. No
per-alternative offsets — rows line up 1:1, affordable because unselected rows
are unread garbage and constant/null storages do not allocate.
`get_alternative<Tag>()` returns one alternative with its `present` mask and
`alternative_mask<Tag>()` just the mask; type checks compose these with the
frame mask, e.g. `mask.and_not(u.alternative_mask<List>())` for the rows that
are not lists, minus `alternative_mask<Null>()` where nulls are legitimate.

`ErasedArray` is the intermediate erasure: a *flat* variant over every physical
storage of every fundamental type plus `Array<List>` and `Array<Record>`, so
there is one tag rather than two.

### Data

The dynamic array and the evaluator's currency. Physically a variant over the
erased alternatives plus `UnionArray`, but presented logically as a variant
over each `Array<Tag>` plus `UnionArray`; hand-written `variant_traits` bridge
the two. A single-alternative union collapses to the concrete array, so
monomorphic columns never pay union dispatch. `null_where(mask)` is where nulls
become structural — it folds into an existing `Array<Null>` alternative or
builds a two-alternative union.

Builders share one vocabulary: `data(v)`, `null()`, `record()`, `list()`,
`none()`/`none_n()`, `length()`, `finish()`. They pad lazily and in bulk — a
field or alternative catches up only when it next receives a value, and in
`finish()` — so skipped rows cost nothing.

## Arrow import

Use `nova::import_arrow_array()` from `<tenzir/nova/arrow_import.hpp>` directly,
without converting through `table_slice` or rebuilding rows. Pass an owning
`std::shared_ptr<arrow::Array>` to transfer eligible buffers; release unnecessary
aliases first and do not use borrowed views after the transfer. Use the
`arrow::Array const&` overload when retaining the original input.

Handle the returned `Result` and keep diagnostics and format-specific conversions
in the reader. Use the Nova overload of `apply_read_pushdown()` for prepared
filters and limits, preserving standalone filtering diagnostics.

## Evaluator

`Events{Array<Record> data, storage::BitMap mask}` is the unit of data;
`length()` is its physical row count and `active_count()` is the number of rows
selected by the mask. Evaluation returns an `Array<Data>`. Rows outside the
requested mask hold unspecified values.

`Evaluator::make` prepares an expression once, holding the owned AST plus one
`CallSite` per `ast::function_call`, keyed by node *address* — hence move-only,
and the expression must never be reallocated. Lambda bodies are part of that
one preparation: their call sites go into the same table, so applying a lambda
is another `EvalRun` over the same expression rather than a nested evaluator.
`eval` builds a throwaway `EvalRun` and a root `EvalFrame` for it.

`apply_kernel` covers the per-row case — a compile-time table of accepted
argument type combinations, warning and returning all-null for the rest without
instantiating the body, unwrapping unions per alternative. `const_eval` is an
`Evaluator` run with no input.

### Contexts

Three context types, each with one job and none of them retained:

- `InstantiateCtx` (diagnostics and registry) is for preparation only.
- `EvalCtx` (diagnostics) is the boundary type. Operators hand it to
  `Evaluator::eval`, where no `EvalRun` exists yet.
- `EvalFrame` is one node's place in an `EvalRun`, and what everything below
  that boundary uses: the `EvalCtx`, the input, the rows to produce, and the
  ability to evaluate subexpressions over them. It converts implicitly to
  `EvalCtx` and `diagnostic_handler&`.

An `EvalRun` is one traversal of a prepared expression over one batch;
`EvalFrame`s are the per-node views into it. The mask lives *in* the frame
rather than travelling as a separate parameter, which turns the central
invariant into a structural one. The rules:

- **A frame's mask is a subset of the input's mask.** The root frame
  establishes this and `narrow` is the only way to obtain a different mask, so
  no node can widen it. `narrow` asserts the subset property.
- **A result is only meaningful for the mask it was produced under.** Rows
  outside `frame.mask()` hold unspecified values; never read them, and never
  expose them.
- **Short-circuiting is mask narrowing, not control flow.** `and`/`or`
  evaluate the right operand under `frame.narrow(rows_that_still_matter)` and
  skip it entirely when that mask is empty. `x if y else z` is one
  special-cased node — hence `null if true else 42` yields `null` — and `else`
  evaluates its right side only for the rows where the left one is null.
  These live in `EvalRun`, not in a function: they need the operand AST,
  which nodes have and functions do not.
- **AST node addresses are stable, so borrowing nodes is sound.** Every
  `ast::expression` holds its node in a `unique_ptr`, so moving an expression
  (or the `Evaluator` owning it) never relocates the nodes below it. This is
  what lets `Evaluator` key function instances by node address. Anything that
  borrows a node must not be copied together with the AST (copying an
  `ast::expression` deep-copies it) and must not outlive the `Evaluator`.
- **A function may be invoked more than once per batch, with a different
  mask each time.** A call site inside the right operand of an `and` is
  evaluated only for the surviving rows, and one inside a lambda body once per
  application. Function state must therefore be independent of batch, mask,
  and input.
- **Function arguments are data, not machinery.** A `ValueArgument` is an
  evaluated argument, a `LambdaArgument` the borrowed lambda node plus its
  capture analysis, and a `LazyArgument` a borrowed expression the function
  evaluates itself. None of them carries an evaluator of its own. Apply it with
  `EvalFrame::eval`,
  which binds the parameter to a subject array and evaluates the body in a
  run over the synthesized input, or with `EvalFrame::eval_elements`, which
  binds it to each element of a list column and broadcasts the captures from
  each row to its elements. Both keep the body's call sites in the enclosing
  evaluator's table.

## Writing new Users

### Event representation invariant

When Nova is enabled, every operator that produces or consumes events must use
`nova::Events`. A `table_slice` in a Nova pipeline indicates that an operator
has not been ported yet; it is not a supported mixed-representation pipeline.

An operator with `nova::Events` input may produce `nova::Events`, bytes, or
`void`, but it must never produce `table_slice`. The same rule applies to
nested pipelines: a nested pipeline that forwards events from a Nova operator
must produce `nova::Events`, while one that acts as a sink may produce `void`.
Do not add `Operator<nova::Events, table_slice>` or an equivalent runtime
transition. Port the remaining operator to Nova or reject the pipeline with a
diagnostic.

### Tests

Migrate tests in place, keeping the existing layout and scenario names. Change
pipelines and baselines only when needed; remove duplicate legacy tests. Do not
add `nova/` or `columnar/` directories or execution-mode filename suffixes.
Do not add or restore legacy-executor tests, even in response to review feedback.

Add or merge this into `tenzir.yaml` in every directory containing migrated TQL
tests, including nested directories:

```yaml
tenzir:
  nova: true
```

Use `tenzir.yaml`, not `test.yaml`, and do not rely on parent configuration
inheritance. In Python tests, pass `--nova=true` to the code under test. Use
`--nova=false` only in separate subprocesses that prepare input when supporting
operators are not ported yet; prefer fixtures where possible.

### Operators

There is no `nova::Operator`. Nova is a fourth `element_type_tag` alternative,
so derive from the ordinary `Operator<Input, Output>` with `nova::Events` in a
slot and register the class as an extra `Impls...` argument to `Describer`,
sharing one `Args` bundle with the `table_slice` implementation. Only `void`,
`chunk_ptr`, `FileHandle`, and `nova::Events` may produce `nova::Events`.
`select_spawn`
prefers the nova implementation when `nova_enabled()`, and errors with
"operator does not support `--nova` yet" when only a `table_slice` one exists.
Keep the legacy implementation unchanged and add a separate Nova implementation,
not a shared templated execution base. Share arguments and registration; isolate
legacy code for eventual removal. Serialize resumable state or explicitly reject
checkpoints. Internal migrations need no changelog or user-facing docs changes.

`Evaluator::make` needs the registry, so build evaluators in `start()` from
`OpCtx` and hold them in `Option`. Per batch, `get_alternative<Bool>` resolves
both the uniformly-boolean and the mixed-type predicate, returning the values
with a mask of the rows that really are `bool`; rows of `input.mask` outside it
had another type. Since `Bool` is stored as a bitmap, the kept rows are then
just an AND, and producing the result is a mask handoff — the record array is
passed through untouched.

```cpp
class WhereEvents final : public Operator<nova::Events, nova::Events> {
  location expr_location_;
  ast::expression expr_;
  Option<nova::Evaluator> evaluator_;

  auto start(OpCtx& ctx) -> Task<void> override {
    auto evaluator = nova::Evaluator::make(
      std::move(expr_), nova::InstantiateCtx{ctx.dh(), ctx.reg()});
    if (not evaluator) {
      co_return;
    }
    evaluator_.emplace(std::move(*evaluator));
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto result = evaluator_->eval(input, nova::EvalCtx{ctx.dh()});
    auto predicate = result.get_alternative<nova::Bool>();
    auto present = predicate
                     ? input.mask & predicate->present
                     : nova::storage::BitMap{input.length(), false};
    if (input.mask.and_not(present).any()) {
      diagnostic::warning("expected `bool`").primary(expr_location_).emit(ctx.dh());
    }
    if (not predicate) {
      co_return;
    }
    auto kept = std::move(present)
                & as<nova::storage::BitMap>(predicate->data.storage());
    if (not kept.any()) {
      co_return;
    }
    input.mask = std::move(kept);
    co_await push(std::move(input));
  }
};
```

A hand-written `ir::Operator` must dispatch itself: return the nova
implementation from `spawn` when the input tag is `nova::Events`, and accept
that tag in `infer_type`. Operators built through `Describer` get this for
free.

### Functions

Functions derive from `nova::FunctionPlugin` and only register their arguments.
The returned `FunctionDescription` validates each concrete call, constant
evaluates the constant arguments, and builds a `CallSite` that owns the
argument bundle and the recipe for filling it. A function itself is a kernel
over that bundle — the framework evaluates the argument expressions for the
requested rows and hands the results over. The kernel is any default
constructible class with an `eval(Args const&, EvalFrame) const` member; there
is no base class, and `FunctionDescriber` checks the shape statically through
the `nova::FunctionImpl` concept.

```cpp
struct SplitArgs {
  nova::ValueArgument x;          // evaluated before every call
  located<std::string> pattern;   // constant, evaluated once
  Option<located<int64_t>> max;   // optional named argument
  bool ignore_case = false;       // flag
  location call;                  // filled by `call_location`
};

class SplitFunction {
public:
  auto eval(SplitArgs const& args, nova::EvalFrame frame) const
    -> nova::EvalResult {
    // `args.x.data` and `args.x.present` are the values for `frame.mask()`,
    // `args.x.source` is where the argument came from. `frame` doubles as the
    // `diagnostic_handler&`.
    // ...
  }
};

class split final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<SplitArgs, SplitFunction>{};
    d.positional("x", &SplitArgs::x, "string");
    d.positional("pattern", &SplitArgs::pattern);
    d.named("max", &SplitArgs::max);
    d.named_optional("ignore_case", &SplitArgs::ignore_case);
    d.call_location(&SplitArgs::call);
    d.validate([](SplitArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      if (args.max and args.max->inner < 0) {
        diagnostic::error("`max` must be at least 0").primary(*args.max).emit(dh);
        return failure::promise();
      }
      return {};
    });
    return std::move(d).finish();
  }
  // `name()`, `is_deterministic()`, and the legacy `make_function()` as usual.
};
```

Key points:

- Argument members are typed after what they receive: `ValueArgument` for an
  evaluated expression, `LambdaArgument` for a unary lambda, `LazyArgument` for
  an expression the function evaluates itself, `ast::field_path` for a
  selector, `located<T>` or
  bare `T` for a constant, `std::vector<...>` for variadics. A bare `T` is
  required, `Option<T>` is not; `optional_positional`/`named_optional` keep a
  bare `T`'s default. An omitted `Option<ValueArgument>` is `None`; an omitted
  `LambdaArgument` or `LazyArgument` reports as false.
- The bundle is owned by the call site, not by the function, and is refilled
  before every call: values are produced for exactly `frame.mask()`, constants
  keep their value from instantiation. One immutable kernel instance serves
  every call site for its implementation type, so implementations are
  stateless — derive anything you want to precompute from constants in
  `validate`, which may store it in `Args`.
- `ValueArgument`s are evaluated eagerly, in registration order, before the
  function runs. A function that must decide *whether* or *for which rows* an
  operand is needed takes a `LazyArgument` instead and evaluates it with
  `frame.eval(args.x)`, optionally on a narrowed frame. Short-circuiting
  operators like `and` work this way, except that they are expression nodes
  rather than functions.
- Instantiation moves the constant arguments out of the call but *borrows*
  everything it evaluates later — values, lambdas, lazy expressions. The call
  therefore has to outlive the call site, which it does in production because
  the `Evaluator` owns the whole expression, and the call sites nested in
  those arguments are prepared by that same evaluator.
- Put all argument checks into `validate`, which receives the materialized
  `Args` and may normalize them; `eval` never fails structurally. Instantiation
  reports every bad argument, skips `validate` if any failed to prepare, and
  stamps errors with the function's usage and docs. Never use
  `argument_parser2` or the legacy `const_eval`.

### Aggregations

Aggregations derive from `nova::AggregationPlugin`, a `function_plugin` with
its own `describe() -> AggregationDescription`, not a `FunctionPlugin`. The
arguments are registered exactly like a function's, through
`AggregationDescriber<Args, Impl>`, whose `finish()` also records how to build
the instance. The implementation is stateful, so it does not fit the shared,
immutable kernel: it is any default constructible class with
`update(Args const&, EvalFrame) -> void`, `get() const -> Data`, and
`reset() -> void`, checked by `nova::AggregationImpl`. `update` folds the
argument values at `frame.mask()` into the state, `get` reads the aggregate
(`Null` before the first `update`), and `reset` starts over without forgetting
anything derived from constants.

```cpp
struct SumArgs {
  nova::ValueArgument x;
};

class SumFunction final
  : public nova::ListFallback<SumFunction, SumArgs, &SumArgs::x> {
public:
  auto update(SumArgs const& args, nova::EvalFrame frame) -> void;
  auto get() const -> nova::Data;
  auto reset() -> void;
};

class plugin : public virtual aggregation_plugin,
               public virtual nova::AggregationPlugin {
  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<SumArgs, SumFunction>{};
    d.positional("x", &SumArgs::x, "number|duration");
    return std::move(d).finish();
  }
  // `name()`, `is_deterministic()`, and the legacy `make_aggregation()`.
};
```

`AggregationInstance::make(expr, ctx)` is the evaluator for one aggregation
call: it fails unless `expr` is a call to an `AggregationPlugin`, prepares the
expression like `Evaluator::make`, and owns the implementation together with
it. Operators feed it batches with `update(events, ctx)`, which evaluates the
arguments for the active rows and hands them to the implementation, and read
the aggregate with `get()`. Only one erasure is involved: `AggregationInstance`
is the abstract type, and the class behind it holds the implementation and the
`Evaluator` directly.

An aggregation in expression position, such as `xs.sum()`, is a regular
function over list rows. `CallSitePreparer` first looks for a `FunctionPlugin`
and then for an `AggregationPlugin`, whose description carries the fallback
kernel: `ListFallback<Derived, Args, Subject>` supplies the `eval` that
`FunctionImpl` expects. It is not an aggregation itself; for every list row of
the `Subject` argument it updates a fresh `Derived` with the row's elements,
reads it, and resets it. A `null` subject propagates silently, an empty list
yields the initial aggregate, and any other type warns and yields `null`. The
elements reach `update` through `EvalFrame::detached(mask, f)`, which runs `f`
with a frame over the list's flat values: a run over a synthesized field-less
input of that length, sharing the evaluator and context. The subject's
`ValueArgument` is replaced by the values column, so an implementation only
ever reads its `Args` at `frame.mask()`.
