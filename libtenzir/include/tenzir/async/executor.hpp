//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// @file async/executor.hpp
/// @brief Pipeline executor infrastructure.

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/async/generator.hpp"
#include "tenzir/async/signal.hpp"
#include "tenzir/result.hpp"

#include <folly/Executor.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Mutex.h>
#include <folly/coro/Synchronized.h>
#include <folly/executors/GlobalExecutor.h>

#include <span>

namespace tenzir {

template <class SemiAwaitable, class F>
auto map_awaitable(SemiAwaitable&& awaitable, F&& f) -> Task<
  std::invoke_result_t<F, folly::coro::semi_await_result_t<SemiAwaitable>>> {
  co_return std::invoke(f, co_await std::forward<SemiAwaitable>(awaitable));
}

// TODO: This might not be cancellation safe?
template <class... SemiAwaitable>
auto select_into_variant(SemiAwaitable&&... awaitable)
  -> Task<variant<folly::coro::semi_await_result_t<SemiAwaitable>...>> {
  using result_t = variant<folly::coro::semi_await_result_t<SemiAwaitable>...>;
  auto [_, result] = co_await folly::coro::collectAny(
    map_awaitable(std::forward<SemiAwaitable>(awaitable), []<class T>(T&& x) {
      return result_t{std::forward<T>(x)};
    })...);
  TENZIR_ASSERT(result.hasValue());
  co_return std::move(*result);
}

/// A sequence of operators with the given input and output.
template <class Input, class Output>
class OperatorChain {
public:
  OperatorChain(const OperatorChain&) = delete;
  OperatorChain& operator=(const OperatorChain&) = delete;
  OperatorChain(OperatorChain&&) = default;
  OperatorChain& operator=(OperatorChain&&) = default;
  ~OperatorChain() = default;

  static auto try_from(std::vector<AnyOperator> operators)
    -> Result<OperatorChain<Input, Output>, std::vector<AnyOperator>> {
    auto input = operator_type{tag_v<Input>};
    for (auto& op : operators) {
      auto next
        = match(op,
                [&]<class In, class Out>(
                  Box<Operator<In, Out>>&) -> std::optional<operator_type> {
                  if (not input.is<In>()) {
                    return std::nullopt;
                  }
                  return tag_v<Out>;
                });
      if (not next) {
        return Err{std::move(operators)};
      }
      input = *next;
    }
    if (not input.is<Output>()) {
      return Err{std::move(operators)};
    }
    return OperatorChain{std::move(operators)};
  }

  auto size() const -> size_t {
    return operators_.size();
  }

  auto operator[](size_t index) const -> const AnyOperator& {
    return operators_[index];
  }

  auto unwrap() && -> std::vector<AnyOperator> {
    return std::move(operators_);
  }

private:
  explicit OperatorChain(std::vector<AnyOperator> operators)
    : operators_{std::move(operators)} {
  }

  std::vector<AnyOperator> operators_;
};

struct PostCommit {};
struct Shutdown {};
struct Stop {};

using FromControl = variant<PostCommit, Shutdown, Stop>;

TENZIR_ENUM(
  /// A message sent from an operator to the controller.
  ToControl,
  /// Notify the host that we are ready to shutdown. After emitting
  /// this, the operator is no longer allowed to send data, so it
  /// should tell its previous operator to stop and its subsequent
  /// operator that it will not get any more input.
  ready_for_shutdown,
  /// Say that we do not want any more input. This will also notify our
  /// preceding operator.
  no_more_input,
  // TODO: Checkpoint messages need data, move into variant.
  /// Inform the controller what checkpoint state we are in.
  checkpoint_begin, checkpoint_done);

// TODO: Where to place this?
template <class T>
class Receiver;
template <class T>
class Sender;

class Never {
private:
  Never() = default;
};

template <class T>
using VoidToNever = std::conditional_t<std::same_as<T, void>, Never, T>;

struct PipeId;
struct ChannelId;

struct OpId {
  std::string value;

  auto sub(size_t index) const -> PipeId;

  auto to(OpId other) const -> ChannelId;

  friend auto format_as(OpId const& self) -> std::string_view {
    return self.value;
  }

  auto operator<=>(OpId const& other) const = default;
};

struct PipeId {
  std::string value;

  auto op(size_t index) const -> OpId {
    return OpId{fmt::format("{}/{}", value, index)};
  }

  friend auto format_as(PipeId const& self) -> std::string_view {
    return self.value;
  }

  auto operator<=>(PipeId const& other) const = default;
};

struct ChannelId {
  std::string value;

  static auto first(OpId id) -> ChannelId {
    return ChannelId{fmt::format("_ -> {}", id.value)};
  }

  static auto last(OpId id) -> ChannelId {
    return ChannelId{fmt::format("{} -> _", id.value)};
  }

  friend auto format_as(ChannelId const& self) -> std::string_view {
    return self.value;
  }

  auto operator<=>(ChannelId const& other) const = default;
};

inline auto OpId::sub(size_t index) const -> PipeId {
  return PipeId{fmt::format("{}-{}", value, index)};
}

inline auto OpId::to(OpId other) const -> ChannelId {
  return ChannelId{fmt::format("{} -> {}", value, other.value)};
}

/// Factory for the channels between operators and optional per-operator
/// executor creation for profiling.
///
/// Implementations need to be thread-safe.
class ExecCtx {
public:
  virtual ~ExecCtx() = default;

  template <class T>
  auto make_channel(ChannelId id) -> PushPull<OperatorMsg<T>> {
    if constexpr (std::same_as<T, void>) {
      return make_void(std::move(id));
    } else if constexpr (std::same_as<T, table_slice>) {
      return make_events(std::move(id));
    } else if constexpr (std::same_as<T, chunk_ptr>) {
      return make_bytes(std::move(id));
    } else {
      static_assert(false, "unknown type");
    }
  }

  /// Returns a per-operator CPU executor.
  virtual auto make_executor(OpId id, std::string name)
    -> folly::Executor::KeepAlive<>
    = 0;

  /// Returns a per-operator IO executor.
  virtual auto make_io_executor(OpId id)
    -> folly::Executor::KeepAlive<folly::IOExecutor>
    = 0;

  /// Returns the metrics receiver actor handle, if available.
  virtual auto metrics_receiver() const -> metrics_receiver_actor = 0;

  /// Create and register a new counter for the pipeline.
  virtual auto make_counter(MetricsLabel label, MetricsDirection direction,
                            MetricsVisibility visibility) -> MetricsCounter
    = 0;

protected:
  virtual auto make_void(ChannelId id) -> PushPull<OperatorMsg<void>> = 0;

  virtual auto make_events(ChannelId id) -> PushPull<OperatorMsg<table_slice>>
    = 0;

  virtual auto make_bytes(ChannelId id) -> PushPull<OperatorMsg<chunk_ptr>> = 0;
};

/// A diagnostic handler that is guaranteed to be thread-safe.
class DiagHandler : public diagnostic_handler {
public:
  virtual auto failure() -> failure_or<void> = 0;
};

/// Run a closed pipeline without external control.
auto run_pipeline(OperatorChain<void, void> pipeline, ExecCtx& exec_ctx,
                  caf::actor_system& sys, DiagHandler& dh) -> Task<void>;

/// Run a right-open pipeline without external control.
template <class Output>
  requires(not std::same_as<Output, void>)
auto run_pipeline(OperatorChain<void, Output> pipeline, caf::actor_system& sys,
                  DiagHandler& dh) -> AsyncGenerator<Output>;

/// Run an open pipeline without external control.
template <class Input, class Output>
  requires(not std::same_as<Output, void> and not std::same_as<Input, void>)
auto run_pipeline_with_input(AsyncGenerator<Input> input,
                             OperatorChain<Input, Output> pipeline,
                             caf::actor_system& sys, DiagHandler& dh)
  -> AsyncGenerator<Output>;

/// Run a pipeline with external control.
template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> pull_upstream,
               Box<Push<OperatorMsg<Output>>> push_downstream,
               Receiver<FromControl> from_control, Sender<ToControl> to_control,
               PipeId id, ExecCtx& exec_ctx, caf::actor_system& sys,
               DiagHandler& dh) -> Task<void>;

} // namespace tenzir

template <>
struct std::hash<tenzir::OpId> {
  auto operator()(tenzir::OpId const& id) const -> size_t {
    return std::hash<std::string>{}(id.value);
  }
};
