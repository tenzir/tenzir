//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

/// Aggregation functions for the nova model. An aggregation plugin registers
/// its arguments like a function, but its implementation class provides two
/// implementations: a stateful accumulator, whose `update` consumes the
/// argument values of one batch, whose `get` reads the aggregate, and whose
/// `reset` starts over; and a static `eval`, the columnar kernel for calls in
/// expression position, which runs over every list row of its subject.
///
/// `Aggregation` is the prepared call: it evaluates the arguments once per
/// batch and folds them into any number of `AggregationState`s, one per
/// group. `AggregationInstance` pairs one with a single state.

#include "tenzir/box.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <concepts>
#include <memory>
#include <span>
#include <string_view>
#include <utility>

namespace tenzir::nova {

/// An `AggregationImpl` that evaluates part of its work once per batch; see
/// there.
template <class Impl, class Args>
concept BatchedAggregationImpl
  = requires(Impl& impl, Args const& args, EvalFrame const& batch_frame,
             EvalFrame&& frame) {
      {
        impl.update(args, Impl::prepare(args, batch_frame), std::move(frame))
      } -> std::same_as<void>;
    };

/// The accumulator half of a nova aggregation: a default constructible type
/// with
///
///     auto update(Args const& args, EvalFrame frame) -> void;
///     auto get() const -> Data;
///     auto reset() -> void;
///
/// `update` folds the argument values at the rows of `frame.mask()` into the
/// state, with diagnostics going to `frame`; `get` reads the aggregate, which
/// is `Null` before the first `update`; `reset` returns to the initial state
/// without forgetting anything derived from constant arguments.
///
/// The arguments are evaluated once per batch, but `update` runs once per
/// group. An aggregation that evaluates more than its arguments, such as a
/// lambda, does so once per batch in a static
///
///     static auto prepare(Args const& args, EvalFrame const& frame) -> Batch;
///
/// over every active row, and replaces `update` with
///
///     auto update(Args const& args, Batch const& batch, EvalFrame frame)
///       -> void;
///
/// whose frame is narrowed from the one `prepare` saw.
template <class Impl, class Args>
concept AggregationImpl
  = std::default_initializable<Impl>
    and (requires(Impl& impl, Args const& args, EvalFrame&& frame) {
          { impl.update(args, std::move(frame)) } -> std::same_as<void>;
        } or BatchedAggregationImpl<Impl, Args>)
    and requires(Impl& impl, Impl const& cimpl) {
          { impl.reset() } -> std::same_as<void>;
          { cimpl.get() } -> std::same_as<Data>;
        };

/// The function half of a nova aggregation: a static member
///
///     static auto eval(Args const& args, EvalFrame frame) -> Array<Data>;
///
/// the columnar kernel for calls in expression position, such as `xs.sum()`.
/// It is static because the kernel is shared by every call site while each
/// group owns its accumulator, so it must not be able to observe one.
/// `aggregate_lists` supplies the list handling it typically needs.
template <class Impl, class Args>
concept AggregationFunctionImpl = requires {
  {
    &Impl::eval
  } -> std::convertible_to<Array<Data> (*)(Args const&, EvalFrame)>;
};

/// The state of one aggregation, e.g., that of one group in `summarize`.
/// Created by `Aggregation::make_state`, and only meaningful to the
/// aggregation that created it.
class AggregationState {
public:
  virtual ~AggregationState() = default;

  /// Reads the aggregate, which is `null` before the first update.
  virtual auto get() const -> Data = 0;

  /// Returns to the initial state.
  virtual auto reset() -> void = 0;
};

/// One group of `Aggregation::update`: the rows to fold, ascending, and the
/// state to fold them into.
struct AggregationGroup {
  Ref<AggregationState> state;
  std::span<storage::Index const> rows;
};

/// A prepared aggregation call: the expression together with its call sites,
/// shared by every state of the call. Grouping is the caller's concern; the
/// aggregation only evaluates the arguments once per batch, regardless of the
/// number of groups.
class Aggregation {
public:
  virtual ~Aggregation() = default;

  /// Prepares `expr`, which must be a call to an `AggregationPlugin`. Emits an
  /// error and fails otherwise, or if the call does not instantiate. Resolves
  /// no secrets, so calls to `secret` fail their validation; this suits
  /// validating a call ahead of execution.
  static auto make(ast::expression expr, InstantiateCtx ctx)
    -> failure_or<Box<Aggregation>>;

  /// Like above, but first resolves the secrets in `expr` through the operator
  /// context, like `Evaluator::make` does for an expression.
  static auto make(ast::expression expr, OpCtx& ctx)
    -> Task<failure_or<Box<Aggregation>>>;

  /// A state in its initial condition.
  virtual auto make_state() const -> Box<AggregationState> = 0;

  /// Evaluates the arguments once for the active rows of `events`, then folds
  /// the rows of every group into its state, in the order of `groups`. Every
  /// row must be active, and every state must stem from this aggregation.
  /// Groups without rows are skipped.
  virtual auto update(Events const& events,
                      std::span<AggregationGroup const> groups, EvalCtx ctx)
    -> void
    = 0;

  /// Folds the active rows of `events` into `state`.
  virtual auto
  update(Events const& events, AggregationState& state, EvalCtx ctx) -> void
    = 0;
};

/// An aggregation with a single state: the evaluator of one aggregation call
/// expression. Feed it batches with `update`, read the aggregate with `get`,
/// and start over with `reset`.
class AggregationInstance {
public:
  /// See `Aggregation::make`.
  static auto make(ast::expression expr, OpCtx& ctx)
    -> Task<failure_or<Box<AggregationInstance>>>;

  explicit AggregationInstance(Box<Aggregation> aggregation)
    : aggregation_{std::move(aggregation)}, state_{aggregation_->make_state()} {
  }

  /// Evaluates the arguments for the active rows of `events` and folds them
  /// into the state.
  auto update(Events const& events, EvalCtx ctx) -> void {
    aggregation_->update(events, *state_, ctx);
  }

  auto get() const -> Data {
    return state_->get();
  }

  auto reset() -> void {
    state_->reset();
  }

private:
  Box<Aggregation> aggregation_;
  Box<AggregationState> state_;
};

namespace _ {

/// The `AggregationState` of `Impl`.
template <class Impl>
class AggregationStateImpl final : public AggregationState {
public:
  auto get() const -> Data override {
    return impl.get();
  }

  auto reset() -> void override {
    impl.reset();
  }

  Impl impl;
};

/// The `Aggregation` of `Impl`: the evaluator, held directly. Constructed
/// through the factory that `AggregationDescriber` records on the call site.
template <class Args, class Impl>
class PreparedAggregation final : public Aggregation {
  static_assert(AggregationImpl<Impl, Args>);

public:
  PreparedAggregation(Evaluator evaluator, ast::function_call const& root)
    : evaluator_{std::move(evaluator)}, root_{std::addressof(root)} {
  }

  auto make_state() const -> Box<AggregationState> override {
    return AggregationStateImpl<Impl>{};
  }

  auto update(Events const& events, std::span<AggregationGroup const> groups,
              EvalCtx ctx) -> void override {
    with_fold(events, ctx, [&](auto const& fold, EvalFrame const& frame) {
      // One mask buffer serves every group: set a group's rows, fold them
      // through a narrowed frame, and clear them again. The narrowed frame
      // releases its copy of the mask when the fold returns, so the buffer is
      // exclusively owned again and reused rather than reallocated.
      auto mask = storage::BitMap::Mutable{events.length()};
      for (auto const& group : groups) {
        if (group.rows.empty()) {
          continue;
        }
        for (auto row : group.rows) {
          mask.set(row, true);
        }
        auto selected = std::move(mask).finish();
        auto& state = static_cast<AggregationStateImpl<Impl>&>(*group.state);
        fold(state.impl, frame.narrow(selected));
        mask = storage::BitMap::Mutable{std::move(selected)};
        for (auto row : group.rows) {
          mask.set(row, false);
        }
      }
    });
  }

  auto update(Events const& events, AggregationState& state, EvalCtx ctx)
    -> void override {
    with_fold(events, ctx, [&](auto const& fold, EvalFrame const& frame) {
      fold(static_cast<AggregationStateImpl<Impl>&>(state).impl, frame);
    });
  }

private:
  /// Evaluates the arguments for the active rows of `events`, runs `prepare`
  /// if `Impl` has one, and calls `f(fold, frame)` with the frame over those
  /// rows. `fold(impl, rows)` folds a frame narrowed from it into `impl`.
  template <class F>
  auto with_fold(Events const& events, EvalCtx ctx, F f) -> void {
    // An empty mask has nothing to fold, and `EvalRun::eval` skips call sites
    // for it as well.
    if (not events.mask.any()) {
      return;
    }
    auto run = EvalRun{evaluator_, std::addressof(events), ctx};
    auto const frame = EvalFrame{run, events.mask};
    // The values are produced for all active rows, so a narrowed frame reads
    // exactly its group's rows of them.
    auto const& args
      = evaluator_.call_site(*root_).fill(frame).template as<Args>();
    if constexpr (BatchedAggregationImpl<Impl, Args>) {
      auto const batch = Impl::prepare(args, frame);
      f(
        [&](Impl& impl, EvalFrame rows) {
          impl.update(args, batch, std::move(rows));
        },
        frame);
    } else {
      f(
        [&](Impl& impl, EvalFrame rows) {
          impl.update(args, std::move(rows));
        },
        frame);
    }
  }

  /// Owns the expression and every call site, including the root's.
  Evaluator evaluator_;
  /// The root call, borrowed from `evaluator_`'s expression. Nodes never
  /// relocate, so moving the evaluator keeps it valid.
  ast::function_call const* root_;
};

} // namespace _

/// The description of an aggregation: the argument registration and the
/// function kernel of a `FunctionDescription`, plus how to build the
/// `Aggregation`. Built with `AggregationDescriber`.
class AggregationDescription : public FunctionDescription {
public:
  /// As `FunctionDescription::instantiate`; the call site additionally
  /// carries the aggregation factory.
  auto instantiate(std::string_view name, ast::function_call& call,
                   InstantiateCtx ctx) const -> failure_or<Instantiation>;

private:
  template <class Args, class Impl>
  friend class AggregationDescriber;

  AggregationDescription(FunctionDescription function,
                         _::CallSite::AggregationFactory factory)
    : FunctionDescription{std::move(function)}, factory_{factory} {
  }

  _::CallSite::AggregationFactory factory_;
};

/// Registers the arguments of a nova aggregation. The registration API is
/// that of `FunctionDescriber`; `finish` yields an `AggregationDescription`.
/// `Impl` must satisfy both `AggregationImpl<Args>` and
/// `AggregationFunctionImpl<Args>`.
template <class Args, class Impl>
class AggregationDescriber : public FunctionDescriber<Args, Impl> {
  static_assert(AggregationImpl<Impl, Args>);
  static_assert(AggregationFunctionImpl<Impl, Args>,
                "aggregations need a static `eval(Args const&, EvalFrame) -> "
                "Array<Data>` for calls in expression position");

public:
  auto finish() && -> AggregationDescription {
    return AggregationDescription{
      static_cast<FunctionDescriber<Args, Impl>&&>(*this).finish(),
      [](Evaluator evaluator,
         ast::function_call const& root) -> Box<Aggregation> {
        return _::PreparedAggregation<Args, Impl>{std::move(evaluator), root};
      },
    };
  }
};

/// A nova aggregation plugin. Deliberately not a `FunctionPlugin`: it has its
/// own `describe`, and the evaluator looks for it after `FunctionPlugin` when
/// preparing a call.
class AggregationPlugin : public virtual function_plugin {
public:
  virtual auto describe() const -> AggregationDescription = 0;

  /// Instantiates `call` against `describe()`, under this plugin's function
  /// name.
  auto instantiate(ast::function_call& call, InstantiateCtx ctx) const
    -> failure_or<FunctionDescription::Instantiation>;
};

/// The elements of one list row: the rows `[begin, end)` of the list column's
/// flat values.
struct ListElements {
  Array<Data> const& values;
  storage::Index begin = 0;
  storage::Index end = 0;

  auto size() const -> storage::Index {
    return end - begin;
  }

  auto empty() const -> bool {
    return begin == end;
  }

  /// Calls `f` with every element in order, as a `RowView<T>` of its type.
  /// The values column is matched once rather than per element, unless it is
  /// a union.
  template <class F>
  auto for_each(F&& f) const -> void {
    match(
      values,
      [&]<data_type Tag>(Array<Tag> const& array) {
        for (auto i = begin; i < end; ++i) {
          f(array.get(i));
        }
      },
      [&](UnionArray const& array) {
        for (auto i = begin; i < end; ++i) {
          match(array.get(i), f);
        }
      });
  }
};

/// The list handling of an aggregation's function kernel: resolves the `list`
/// rows of `subject` at `frame.mask()` and builds the result by calling
/// `f(ListElements, ArrayBuilder<Data>&)` for each of them, which must append
/// exactly one row. A `null` subject propagates silently, any other type warns
/// and yields `null`. A constant list column is aggregated once.
template <class F>
auto aggregate_lists(ValueArgument const& subject, EvalFrame const& frame, F f)
  -> Array<Data> {
  auto const& mask = frame.mask();
  auto lists = subject.data.get_alternative<List>();
  auto list_rows
    = lists ? mask & lists->present : storage::BitMap{frame.length(), false};
  auto bad = mask.and_not(list_rows);
  if (auto nulls = subject.data.get_alternative<Null>()) {
    bad = std::move(bad).and_not(nulls->present);
  }
  if (bad.any()) {
    diagnostic::warning("expected `list`, got a different type")
      .primary(subject.source)
      .emit(frame);
  }
  if (not lists or not list_rows.any()) {
    return frame.null();
  }
  return match(
    lists->data.storage(),
    [&](storage::ListStorage const& storage) -> Array<Data> {
      auto const& values = storage.values();
      auto const& spans = storage.spans();
      auto builder = ArrayBuilder<Data>{};
      for (auto row : storage::true_bits(mask)) {
        builder.skip_n(row - builder.length());
        if (not list_rows.get(row)) {
          builder.null();
          continue;
        }
        auto const [begin, end] = spans[row];
        f(ListElements{values, begin, end}, builder);
      }
      builder.skip_n(frame.length() - builder.length());
      return builder.finish();
    },
    [&](storage::ConstantStorage<List, RowView<List>> const& storage)
      -> Array<Data> {
      auto elements = ArrayBuilder<Data>{};
      for (auto const& element : storage.value()) {
        append_data(elements, element);
      }
      auto const length = elements.length();
      auto const values = elements.finish();
      auto builder = ArrayBuilder<Data>{};
      f(ListElements{values, 0, length}, builder);
      return repeat(builder.take_last(), frame.length())
        .null_where(mask.and_not(list_rows));
    });
}

} // namespace tenzir::nova
