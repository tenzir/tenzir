//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

/// Aggregation functions for the nova model. An aggregation plugin registers
/// its arguments like a function, but its implementation is stateful:
/// `update` consumes the argument values of one batch, `get` reads the
/// aggregate, and `reset` starts over. `AggregationInstance` is the evaluator
/// that owns one such implementation together with the prepared expression.
/// Invoked in expression position, an aggregation runs over every list row of
/// its subject argument instead; `ListFallback` supplies that `eval`.

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
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <concepts>
#include <memory>
#include <string_view>
#include <utility>

namespace tenzir::nova {

/// The implementation of a nova aggregation: a default constructible type
/// with
///
///     auto update(Args const& args, EvalFrame frame) -> void;
///     auto get() const -> Data;
///     auto reset() -> void;
///
/// `update` folds the argument values at the rows of `frame.mask()` into the
/// state, with diagnostics going to `frame`; `get` reads the aggregate, which
/// is `Null` before the first `update`; `reset` returns to the initial state
/// without forgetting anything derived from constant arguments. To also serve
/// as a regular function, derive from `ListFallback`.
template <class Impl, class Args>
concept AggregationImpl = std::default_initializable<Impl>
                          and requires(Impl& impl, Impl const& cimpl,
                                       Args const& args, EvalFrame&& frame) {
                                {
                                  impl.update(args, std::move(frame))
                                } -> std::same_as<void>;
                                { impl.reset() } -> std::same_as<void>;
                                { cimpl.get() } -> std::same_as<Data>;
                              };

/// The evaluator of one aggregation call expression: the prepared expression
/// plus the implementation's state. Feed it batches with `update`, read the
/// aggregate with `get`, and start over with `reset`.
class AggregationInstance {
public:
  virtual ~AggregationInstance() = default;

  /// Prepares `expr`, which must be a call to an `AggregationPlugin`. Emits an
  /// error and fails otherwise, or if the call does not instantiate.
  static auto make(ast::expression expr, InstantiateCtx ctx)
    -> failure_or<Box<AggregationInstance>>;

  /// Evaluates the arguments for the active rows of `events` and folds them
  /// into the state.
  virtual auto update(Events const& events, EvalCtx ctx) -> void = 0;

  virtual auto get() const -> Data = 0;

  virtual auto reset() -> void = 0;
};

namespace _ {

/// The `AggregationInstance` of `Impl`: the state and the evaluator, held
/// directly. Constructed through the factory that `AggregationDescriber`
/// records on the call site.
template <class Args, class Impl>
class AggregationInstanceImpl final : public AggregationInstance {
  static_assert(AggregationImpl<Impl, Args>);

public:
  AggregationInstanceImpl(Evaluator evaluator, ast::function_call const& root)
    : evaluator_{std::move(evaluator)}, root_{std::addressof(root)} {
  }

  auto update(Events const& events, EvalCtx ctx) -> void override {
    // An empty mask has nothing to fold, and `EvalRun::eval` skips call sites
    // for it as well.
    if (not events.mask.any()) {
      return;
    }
    auto run = EvalRun{evaluator_, std::addressof(events), ctx};
    auto frame = EvalFrame{run, events.mask};
    auto const& args
      = evaluator_.call_site(*root_).fill(frame).template as<Args>();
    impl_.update(args, std::move(frame));
  }

  auto get() const -> Data override {
    return impl_.get();
  }

  auto reset() -> void override {
    impl_.reset();
  }

private:
  /// Owns the expression and every call site, including the root's.
  Evaluator evaluator_;
  /// The root call, borrowed from `evaluator_`'s expression. Nodes never
  /// relocate, so moving the evaluator keeps it valid.
  ast::function_call const* root_;
  Impl impl_;
};

} // namespace _

/// The description of an aggregation: the argument registration and the
/// regular-function fallback kernel of a `FunctionDescription`, plus how to
/// build the `AggregationInstance`. Built with `AggregationDescriber`.
class AggregationDescription : public FunctionDescription {
public:
  /// As `FunctionDescription::instantiate`; the call site additionally
  /// carries the instance factory.
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
/// `Impl` must satisfy `AggregationImpl<Args>` and, as the fallback kernel,
/// `FunctionImpl<Args>`, which `ListFallback` provides.
template <class Args, class Impl>
class AggregationDescriber : public FunctionDescriber<Args, Impl> {
  static_assert(AggregationImpl<Impl, Args>);

public:
  auto finish() && -> AggregationDescription {
    return AggregationDescription{
      static_cast<FunctionDescriber<Args, Impl>&&>(*this).finish(),
      [](Evaluator evaluator,
         ast::function_call const& root) -> Box<AggregationInstance> {
        return _::AggregationInstanceImpl<Args, Impl>{std::move(evaluator),
                                                      root};
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

/// Makes an aggregation implementation usable as a regular function: the
/// `eval` that `FunctionImpl` expects. It is not an aggregation itself. For
/// every list row of the `Subject` argument, a fresh `Derived` is updated with
/// the row's elements, read, and reset; the results form the output column. A
/// `null` subject propagates silently, an empty list yields the initial
/// aggregate, and any other type warns and yields `null`.
///
/// The elements are presented to `update` under a detached frame over the
/// list's flat values (see `EvalFrame::detached`), so an implementation only
/// ever sees its `Args` and `frame.mask()`. Lambda and lazy arguments
/// evaluated under that frame see a field-less input.
template <class Derived, class Args, ValueArgument Args::* Subject>
class ListFallback {
public:
  auto eval(Args const& args, EvalFrame frame) const -> Array<Data> {
    auto const& subject = args.*Subject;
    auto const& mask = frame.mask();
    auto lists = subject.data.template get_alternative<List>();
    auto list_rows
      = lists ? mask & lists->present : storage::BitMap{frame.length(), false};
    auto bad = mask.and_not(list_rows);
    if (auto nulls = subject.data.template get_alternative<Null>()) {
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
        return aggregate_rows(args, storage, list_rows, frame);
      },
      [&](storage::ConstantStorage<List, RowView<List>> const& storage)
        -> Array<Data> {
        return aggregate_constant(args, storage, list_rows, frame);
      });
  }

private:
  /// Every row of `frame.mask()`: the aggregate of its elements for the rows
  /// of `list_rows`, `null` for the others.
  static auto
  aggregate_rows(Args const& args, storage::ListStorage const& storage,
                 storage::BitMap const& list_rows, EvalFrame const& frame)
    -> Array<Data> {
    auto const& values = storage.values();
    auto const& spans = storage.spans();
    auto local = args;
    local.*Subject = ValueArgument{values, (args.*Subject).source};
    auto impl = Derived{};
    auto builder = ArrayBuilder<Data>{};
    // One element mask, reused across rows: after a row's elements were
    // consumed, its bits are cleared again instead of allocating a new mask.
    auto elements = storage::BitMap::Mutable{values.length()};
    for (auto row : storage::true_bits(frame.mask())) {
      builder.skip_n(row - builder.length());
      if (not list_rows.get(row)) {
        builder.null();
        continue;
      }
      auto const [begin, end] = spans[row];
      if (begin != end) {
        for (auto i = begin; i < end; ++i) {
          elements.set(i, true);
        }
        auto selected = std::move(elements).finish();
        frame.detached(selected, [&](EvalFrame inner) {
          impl.update(local, std::move(inner));
        });
        // The detached run has released its copies, so the buffer is
        // exclusively owned again and taken over rather than copied.
        elements = storage::BitMap::Mutable{std::move(selected)};
        for (auto i = begin; i < end; ++i) {
          elements.set(i, false);
        }
      }
      append_data(builder, impl.get());
      impl.reset();
    }
    builder.skip_n(frame.length() - builder.length());
    return builder.finish();
  }

  /// Every list row holds the same list, so the aggregate is computed once.
  static auto aggregate_constant(
    Args const& args,
    storage::ConstantStorage<List, RowView<List>> const& storage,
    storage::BitMap const& list_rows, EvalFrame const& frame) -> Array<Data> {
    auto const& list = storage.value();
    auto impl = Derived{};
    if (not list.empty()) {
      auto builder = ArrayBuilder<Data>{};
      for (auto const& element : list) {
        append_data(builder, element);
      }
      auto local = args;
      local.*Subject = ValueArgument{builder.finish(), (args.*Subject).source};
      auto const length = static_cast<storage::Index>(list.size());
      frame.detached(storage::BitMap{length, true}, [&](EvalFrame inner) {
        impl.update(local, std::move(inner));
      });
    }
    return repeat(impl.get(), frame.length())
      .null_where(frame.mask().and_not(list_rows));
  }
};

} // namespace tenzir::nova
