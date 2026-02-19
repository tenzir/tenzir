//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// @file async.hpp
/// @brief Pipeline executor API for implementing operators.
///
/// ## Operator Base Class
///
/// Operators derive from `Operator<Input, Output>` where Input/Output are
/// `void`, `table_slice`, or `chunk_ptr`. The executor calls methods based on
/// operator type.
///
/// ## Lifecycle for Non-Source Operators (Input != void)
///
/// 1. `process(input, push, ctx)` - Called for each incoming data item
/// 2. `state()` - Polled to check if operator is done
///
/// ## Common Methods (all operator types)
///
/// - `await_task()` - Returns Task<Any> the executor awaits. Sources use
///   this to produce data; non-sources typically sleep forever (the default).
/// - `process_task(result, push, ctx)` - Called when `await_task()` completes.
/// - `finalize(push, ctx)` - Called exactly once when upstream signals
///   end-of-data. Use for buffering operators (tail, sort, aggregations) that
///   must see all input before producing output.
/// - `state()` - Polled after processing; return `done` for early completion.
///
/// ## Key Invariants
///
/// - `process()` is only called when input is available; operators cannot rely
///   on periodic invocation.
/// - Buffering operators may `co_return` from `process()` without pushing.
/// - Sources use `await_task()` to drive data production. To run indefinitely,
///   never return `done` from `state()`.
/// - `snapshot()` handles both serialization and deserialization via `Serde`.

#pragma once

#include "tenzir/any.hpp"
#include "tenzir/async/push_pull.hpp"
#include "tenzir/async/scope.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/result.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace tenzir {

class base_ctx;

namespace ir {
struct pipeline;
} // namespace ir

// TODO: User proper types for this?
using SubKey = data;
using SubKeyView = data_view;

template <class T>
struct OperatorMsg;

template <class Input>
class OpenPipeline {
public:
  explicit OpenPipeline(Push<OperatorMsg<Input>>& push);

  template <std::same_as<Input> In>
  auto push(In input) -> Task<Result<void, In>>;
  auto close() -> Task<void>;

private:
  Ref<Push<OperatorMsg<Input>>> push_;
};

using AnyOpenPipeline = variant<OpenPipeline<void>, OpenPipeline<chunk_ptr>,
                                OpenPipeline<table_slice>>;

class OpCtx {
public:
  virtual ~OpCtx() = default;

  explicit(false) operator diagnostic_handler&() {
    return dh();
  }

  explicit(false) operator base_ctx();

  virtual auto actor_system() -> caf::actor_system& = 0;
  /// Returns the diagnostic handler. The returned handler is guaranteed to be
  /// thread-safe.
  virtual auto dh() -> diagnostic_handler& = 0;
  virtual auto reg() -> const registry& = 0;
  virtual auto resolve_secrets(std::vector<secret_request> requests)
    -> Task<failure_or<void>>
    = 0;
  virtual auto spawn_sub(SubKey key, ir::pipeline pipe, element_type_tag input)
    -> Task<AnyOpenPipeline>
    = 0;
  virtual auto get_sub(SubKeyView key) -> std::optional<AnyOpenPipeline> = 0;
  // TODO: Change `void` to `Any`.
  virtual auto spawn_task(Task<void> task) -> AsyncHandle<void> = 0;
  virtual auto save_checkpoint(chunk_ptr chunk) -> Task<void> = 0;
  virtual auto load_checkpoint() -> Task<chunk_ptr> = 0;
  virtual auto flush() -> Task<void> = 0;

  template <class Awaitable>
    requires std::is_void_v<folly::coro::semi_await_result_t<Awaitable>>
  auto spawn_task(Awaitable&& awaitable) -> AsyncHandle<void> {
    return spawn_task(
      [awaitable = std::forward<Awaitable>(awaitable)] mutable -> Task<void> {
        co_await std::move(awaitable);
      });
  }

  template <class F>
    requires std::is_void_v<
      folly::coro::semi_await_result_t<std::invoke_result_t<F>>>
  auto spawn_task(F f) -> AsyncHandle<void> {
    return spawn_task(folly::coro::co_invoke(std::move(f)));
  }
};

enum class OperatorState {
  /// The operator doesn't request any specific state.
  unspecified,
  /// The operator wants to finalize.
  done,
  /// The operator is draining and will manually set the state
  /// to `done` when finished. The operator can opt-into this
  /// behavior by setting the state to `almost_done` in `finalize()`,
  /// the executor will then continue scheduling the pipeline normally,
  /// and the operator is responsible for switching to `done` within
  /// a bounded amount of time.
  almost_done,
};

template <class Input, class Output>
class OperatorInputOutputBase {
public:
  /// Process a single input item. See file-level docs for invariants.
  virtual auto process(Input input, Push<Output>& push, OpCtx& ctx)
    -> Task<void>
    = 0;

protected:
  ~OperatorInputOutputBase() = default;
};

template <class Output>
class OperatorInputOutputBase<void, Output> {};

template <class Input>
class OperatorInputOutputBase<Input, void> {
public:
  virtual auto process(Input input, OpCtx& ctx) -> Task<void> = 0;

protected:
  ~OperatorInputOutputBase() = default;
};

template <class Output>
class OperatorOutputBase {
public:
  /// Process result of `await_task()` for sources. See file-level docs.
  virtual auto process_task(Any result, Push<Output>& push, OpCtx& ctx)
    -> Task<void> {
    TENZIR_UNUSED(result, push, ctx);
    TENZIR_ERROR("ignoring task result in {}", typeid(*this).name());
    co_return;
  }

  /// Called once at end-of-stream. See file-level docs.
  virtual auto finalize(Push<Output>& push, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(push, ctx);
    co_return;
  }

  /// Process the result of a spawned subpipeline in a *thread-safe* way.
  ///
  /// Note that, unlike all other functions in the operator interface, this one
  /// may be called in parallel while another call is active.
  virtual auto process_sub(SubKeyView key, table_slice slice,
                           Push<Output>& push, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(key, ctx);
    if constexpr (std::same_as<Output, table_slice>) {
      co_await push(std::move(slice));
    } else {
      panic("subpipeline result handling is not implemented for this operator");
    }
  }

  /// This is *not* required to be thread-safe.
  virtual auto finish_sub(SubKeyView key, Push<Output>& push, OpCtx& ctx)
    -> Task<void> {
    TENZIR_UNUSED(key, push, ctx);
    // We don't panic here since this is a reasonable default implementation.
    co_return;
  }

protected:
  ~OperatorOutputBase() = default;
};

template <>
class OperatorOutputBase<void> {
public:
  virtual auto process_task(Any result, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(result, ctx);
    TENZIR_ERROR("ignoring task result in {}", typeid(*this).name());
    co_return;
  }

  virtual auto finalize(OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(ctx);
    co_return;
  }

  virtual auto process_sub(SubKeyView key, table_slice slice, OpCtx& ctx)
    -> Task<void> {
    TENZIR_UNUSED(key, slice, ctx);
    TENZIR_UNREACHABLE();
  }

  virtual auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(key, ctx);
    co_return;
  }

protected:
  ~OperatorOutputBase() = default;
};

// TODO: CAF binary format might not be the best choice. What properties and
// guarantees do we need?
class Serde {
public:
  /// Construct an instance for deserializing.
  explicit Serde(caf::binary_deserializer& f) : f_{f} {
  }

  // Construct an instance for serializing.
  explicit Serde(caf::binary_serializer& f) : f_{f} {
  }

  template <class T>
  auto operator()(std::string_view name, T& value) {
    auto success = match(f_, [&](auto& f) {
      return f->field(name, value)(*f);
    });
    TENZIR_ASSERT(success);
  }

private:
  variant<Ref<caf::binary_serializer>, Ref<caf::binary_deserializer>> f_;
  chunk_ptr chunk_;
};

class OperatorBase {
public:
  virtual auto start(OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(ctx);
    co_return;
  }

  /// Serialize/deserialize state for checkpointing. See file-level docs.
  virtual auto snapshot(Serde& serde) -> void {
    TENZIR_UNUSED(serde);
  }

  /// Return task for sources to await. See file-level docs.
  virtual auto await_task(diagnostic_handler& dh) const -> Task<Any> {
    TENZIR_UNUSED(dh);
    co_await wait_forever();
    TENZIR_UNREACHABLE();
  }

  virtual auto post_commit(OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(ctx);
    co_return;
  }

  /// Return operator state. See file-level docs.
  virtual auto state() -> OperatorState {
    return OperatorState::unspecified;
  }

  /// Called to signal that a source should stop producing data.
  virtual auto stop(OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(ctx);
    co_return;
  }

protected:
  ~OperatorBase() = default;
};

template <class Input, class Output>
class Operator : public OperatorBase,
                 public OperatorOutputBase<Output>,
                 public OperatorInputOutputBase<Input, Output> {
public:
  virtual ~Operator() = default;
};

using AnyOperator = variant<
  Box<Operator<void, chunk_ptr>>, Box<Operator<void, table_slice>>,
  Box<Operator<chunk_ptr, chunk_ptr>>, Box<Operator<chunk_ptr, table_slice>>,
  Box<Operator<table_slice, chunk_ptr>>, Box<Operator<table_slice, table_slice>>,
  Box<Operator<table_slice, void>>, Box<Operator<chunk_ptr, void>>>;

} // namespace tenzir
