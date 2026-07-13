//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// @file async/executor.hpp
/// @brief Pipeline executor infrastructure.

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/async/channel.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/signal.hpp"
#include "tenzir/result.hpp"

#include <folly/Executor.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Mutex.h>
#include <folly/coro/Synchronized.h>
#include <folly/executors/GlobalExecutor.h>

#include <functional>

namespace tenzir {

namespace ir {
struct Plan;
} // namespace ir

struct PostCommit {};

/// Request a graceful shutdown of the operator.
///
/// For source operators (Input == void), calls `stop()` so the source can
/// finish in-flight work and then proceed to `finalize()` naturally.
/// Non-source operators ignore this signal — they drain via end-of-data
/// propagation from upstream.
struct GracefulStop {};

/// Cancel everything that the inner operator implementation is doing.
///
/// This does not stop the runner itself as we need to continue to forward
/// checkpoints. Note that this currently only cancels the inner work when
/// getting the chance to read from the control channel. Ideally, we would react
/// to this signal immediately, but we'll leave that for later.
struct HardStop {};

using FromControl = variant<PostCommit, GracefulStop, HardStop>;

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

  template <class T>
  auto make_fused_channel(ChannelId id) -> PushPull<OperatorMsg<T>> {
    if constexpr (std::same_as<T, void>) {
      return make_fused_void(std::move(id));
    } else if constexpr (std::same_as<T, table_slice>) {
      return make_fused_events(std::move(id));
    } else if constexpr (std::same_as<T, chunk_ptr>) {
      return make_fused_bytes(std::move(id));
    } else {
      static_assert(false, "unknown type");
    }
  }

  /// Creates an events channel for a routing exchange (scatter, gather,
  /// shuffle, or broadcast). Routing channels use a dedicated per-channel
  /// limit and are expected to share one `ChannelId` across all lanes of an
  /// exchange so their profiling stats collate into a single metric.
  auto make_routing_channel(ChannelId id)
    -> PushPull<OperatorMsg<table_slice>> {
    return make_routing_events(std::move(id));
  }

  /// Returns a per-operator CPU executor.
  virtual auto make_executor(OpId id, std::string name)
    -> folly::Executor::KeepAlive<>
    = 0;

  /// Returns a per-operator IO executor.
  virtual auto make_io_executor(OpId id, std::string name)
    -> folly::Executor::KeepAlive<folly::IOExecutor>
    = 0;

  /// Returns the metrics receiver actor handle, if available.
  virtual auto metrics_receiver() const -> metrics_receiver_actor = 0;

  /// Create and register a new counter for the pipeline.
  virtual auto make_counter(MetricsLabel label, MetricsDirection direction,
                            MetricsVisibility visibility, MetricsUnit type)
    -> MetricsCounter
    = 0;

  /// Returns whether the pipeline is hidden.
  virtual auto is_hidden() const -> bool = 0;

  /// Returns whether the operator has access to an interactive terminal.
  virtual auto has_terminal() const -> bool = 0;

  /// Returns the (immutable) checkpoint settings of the pipeline.
  virtual auto checkpoint_settings() const -> Option<CheckpointSettings const&>
    = 0;

protected:
  virtual auto make_void(ChannelId id) -> PushPull<OperatorMsg<void>> = 0;

  virtual auto make_events(ChannelId id) -> PushPull<OperatorMsg<table_slice>>
    = 0;

  virtual auto make_routing_events(ChannelId id)
    -> PushPull<OperatorMsg<table_slice>>
    = 0;

  virtual auto make_bytes(ChannelId id) -> PushPull<OperatorMsg<chunk_ptr>> = 0;

  virtual auto make_fused_void(ChannelId id) -> PushPull<OperatorMsg<void>> = 0;

  virtual auto make_fused_events(ChannelId id)
    -> PushPull<OperatorMsg<table_slice>>
    = 0;

  virtual auto make_fused_bytes(ChannelId id)
    -> PushPull<OperatorMsg<chunk_ptr>>
    = 0;
};

/// A diagnostic handler that is guaranteed to be thread-safe.
class DiagHandler : public diagnostic_handler {
public:
  virtual auto failure() -> failure_or<void> = 0;
};

/// Drives an already-wired plan to completion. The caller owns all IO: the
/// upstream/downstream endpoints and the control channels. Higher-level
/// entry points (`execute_plan`, `execute_plan_with_io`) create that plumbing
/// and then call this primitive.
///
/// When `fused` is set, the chain runs with fused channels: each input is fully
/// processed through the entire chain before the next input is pulled.
template <class Input, class Output>
auto drive_plan(ir::Plan plan, Box<Pull<OperatorMsg<Input>>> pull_upstream,
                Box<Push<OperatorMsg<Output>>> push_downstream,
                Receiver<FromControl> from_control,
                Sender<ToControl> to_control, PipeId id, ExecCtx& exec_ctx,
                caf::actor_system& sys, DiagHandler& dh, bool fused = false)
  -> Task<void>;

/// Runs a pipeline plan without external control.
///
/// Creates the input/output channels and the control operators, then drives
/// the plan, handling graceful stop and control messages.
auto execute_plan(ir::Plan plan, ExecCtx& exec_ctx, caf::actor_system& sys,
                  DiagHandler& dh, Notify* graceful_stop = nullptr)
  -> Task<void>;

/// Feeds input into a bounded pipeline.
using PipelineFeeder
  = std::function<Task<void>(Push<OperatorMsg<table_slice>>&)>;

/// Drains output from a bounded pipeline.
using PipelineDrainer
  = std::function<Task<void>(Pull<OperatorMsg<table_slice>>&)>;

/// Runs a pipeline plan that pull `table_slice` and push `table_slice`.
///
/// Creates the input/output channels and the control operators, then drives
/// the plan, handling graceful stop and control messages.
auto execute_plan_with_io(ir::Plan plan, ExecCtx& exec_ctx,
                          caf::actor_system& sys, DiagHandler& dh,
                          PipelineFeeder feed_input,
                          PipelineDrainer drain_output) -> Task<void>;

} // namespace tenzir

template <>
struct std::hash<tenzir::OpId> {
  auto operator()(tenzir::OpId const& id) const -> size_t {
    return std::hash<std::string>{}(id.value);
  }
};
