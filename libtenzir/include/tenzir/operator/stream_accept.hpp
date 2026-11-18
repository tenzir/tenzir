//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/async/semaphore.hpp"
#include "tenzir/async/stream.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/base_ctx.hpp"
#include "tenzir/box.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <functional>
#include <limits>
#include <string>
#include <unordered_map>

namespace tenzir {

namespace detail {

/// One accepted connection, handed to the subpipeline that reads it.
struct StreamConnection {
  /// The transport to read from. `None` if the connection no longer exists,
  /// which is the case for every connection from before a restart.
  Option<Arc<folly::coro::Transport>> transport;
  /// Records the size of every chunk read from the transport.
  std::function<void(size_t)> record_read;
  /// Receives the reason when reading fails. The accepting operator reports it
  /// once the subpipeline finishes.
  Arc<Mutex<Option<std::string>>> read_error;
  /// The name and location under which the source shows up in diagnostics and
  /// pipeline insights.
  std::string name;
  location source;
};

/// Reads one accepted connection at the start of its subpipeline.
///
/// Each connection reads in its own subpipeline, so a connection whose
/// subpipeline cannot keep up only stops reading from its own peer. The
/// accepting operator never pushes into the subpipeline, which would block it
/// for all connections.
///
/// A connection without a transport has nothing left to read, so its source
/// ends right away. The operators after it then drain the state they restored.
class StreamConnectionSource final : public Operator<void, chunk_ptr> {
public:
  explicit StreamConnectionSource(StreamConnection connection)
    : transport_{std::move(connection.transport)},
      record_read_{std::move(connection.record_read)},
      read_error_{std::move(connection.read_error)},
      done_{transport_.is_none()} {
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (not transport_) {
      // There is nothing to read, and `state()` already reports `done`.
      co_await wait_forever();
    }
    auto& transport = **transport_;
    auto* evb = transport.getEventBase();
    TENZIR_ASSERT(evb);
    try {
      co_return co_await folly::coro::co_withExecutor(
        evb, read_stream_chunk(transport, buffer_size,
                               std::chrono::milliseconds{0}));
    } catch (folly::AsyncSocketException const& ex) {
      co_return std::string{ex.what()};
    }
  }

  auto process_task(Any result, Push<chunk_ptr>& push, OpCtx&)
    -> Task<void> override {
    if (auto* error = result.try_as<std::string>()) {
      *co_await read_error_->lock() = std::move(*error);
      done_ = true;
      co_return;
    }
    auto chunk = std::move(result).as<Option<chunk_ptr>>();
    if (not chunk) {
      done_ = true;
      co_return;
    }
    record_read_((*chunk)->size());
    co_await push(std::move(*chunk));
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto stop(OpCtx&) -> Task<void> override {
    if (transport_) {
      // Closing the transport ends the pending read, which finishes the
      // source.
      close_stream_transport(*transport_);
    }
    co_return;
  }

private:
  static constexpr auto buffer_size = size_t{64 * 1024};

  /// Reading happens in `await_task()`, which is `const`.
  mutable Option<Arc<folly::coro::Transport>> transport_;
  std::function<void(size_t)> record_read_;
  Arc<Mutex<Option<std::string>>> read_error_;
  bool done_;
};

/// The IR operator that `StreamAccept` places in front of the user's pipeline
/// for each accepted connection.
///
/// A connection does not survive a restart, but its subpipeline must: it gets
/// restored without a transport so that it can drain the state from its
/// snapshot. Data that arrived after the last checkpoint is lost: the kernel
/// already acknowledged it, so the peer does not send it again.
///
/// TODO: Add a serialization plugin that persists everything except the
/// transport once the executor checkpoints subpipelines. Until then, this
/// operator never gets serialized.
class StreamConnectionIr final : public ir::Operator {
public:
  explicit StreamConnectionIr(StreamConnection connection)
    : connection_{std::move(connection)} {
  }

  auto name() const -> std::string override {
    return "stream_connection_ir";
  }

  auto display_name() const -> std::string override {
    return connection_.name;
  }

  auto main_location() const -> location override {
    return connection_.source;
  }

  auto copy() const -> Box<ir::Operator> override {
    return StreamConnectionIr{connection_};
  }

  auto move() && -> Box<ir::Operator> override {
    return StreamConnectionIr{std::move(connection_)};
  }

  auto infer_type(element_type_tag, base_ctx) const
    -> failure_or<element_type_tag> override {
    return tag_v<chunk_ptr>;
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto spawn(element_type_tag, registry const&) const -> AnyOperator override {
    return Box<tenzir::Operator<void, chunk_ptr>>{
      StreamConnectionSource{connection_}.with_name(connection_.name)};
  }

private:
  StreamConnection connection_;
};

} // namespace detail

template <class Impl>
class StreamAccept final : public Operator<void, nova::Events> {
public:
  using Args = typename Impl::Args;
  using AcceptedInfo = typename Impl::AcceptedInfo;
  using ConnectionState = typename Impl::ConnectionState;
  using Connection = Arc<folly::coro::Transport>;

  struct Accepted {
    AcceptedInfo info;
  };

  struct AcceptLoopFinished {};

  struct ConnectionReleased {};

  using Message = variant<AcceptLoopFinished, ConnectionReleased, Accepted>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  explicit StreamAccept(Args args)
    : impl_{std::move(args)},
      max_connections_{impl_.max_connections()},
      connection_slots_{detail::narrow<size_t>(max_connections_)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await impl_.prepare(ctx)) {
      request_abort();
      co_return;
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    if (not co_await impl_.start_listener(evb_, ctx)) {
      request_abort();
      co_return;
    }
    events_read_counter_
      = ctx.make_counter(impl_.events_metric_label(), MetricsDirection::read,
                         MetricsVisibility::external_, MetricsUnit::events);
    ctx.spawn_task([this, &ctx]() -> Task<void> {
      auto notify_finished = detail::scope_guard{[this, &ctx]() noexcept {
        ctx.spawn_task([this]() -> Task<void> {
          co_await message_queue_->enqueue(AcceptLoopFinished{});
        });
      }};
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token,
        accept_cancel_->getToken());
      co_await folly::coro::co_withCancellation(token, accept_loop(ctx));
    });
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](Accepted accepted) -> Task<void> {
        auto transport = std::move(accepted.info.transport);
        if (lifecycle_ != Lifecycle::running) {
          close_stream_transport(std::move(transport));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        auto conn_id = next_conn_id_++;
        auto state
          = impl_.make_connection_state(*transport, accepted.info, ctx);
        auto bytes_read_counter
          = ctx.make_counter(impl_.bytes_metric_label(accepted.info),
                             MetricsDirection::read,
                             MetricsVisibility::external_, MetricsUnit::bytes);
        auto read_error = Arc<Mutex<Option<std::string>>>{std::in_place};
        auto pipeline_copy = impl_.pipeline().inner;
        impl_.bind(pipeline_copy, accepted.info, ctx);
        // The subpipeline reads the connection itself. This way, a connection
        // whose subpipeline cannot keep up never blocks the other connections.
        pipeline_copy.operators.insert(
          pipeline_copy.operators.begin(),
          detail::StreamConnectionIr{detail::StreamConnection{
            .transport = transport,
            .record_read =
              [state, bytes_read_counter](size_t bytes) mutable {
                bytes_read_counter.add(bytes);
                Impl::record_read(state, bytes);
              },
            .read_error = read_error,
            .name = std::string{impl_.debug_name()},
            .source = impl_.pipeline().source,
          }});
        if (not co_await ctx.plan_and_spawn_sub<void>(
              sub_key_for(conn_id), std::move(pipeline_copy),
              DiagnosticBehavior::ErrorToWarning)) {
          Impl::close_connection_state(state);
          close_stream_transport(std::move(transport));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        // The connection keeps its slot until its subpipeline finishes.
        // Otherwise, subpipelines of closed connections could pile up without
        // bound while they wait for downstream to accept their output.
        auto [_, inserted] = connections_.emplace(
          conn_id, ConnectionEntry{std::move(transport), std::move(state),
                                   std::move(read_error)});
        TENZIR_ASSERT(inserted);
      },
      [&](AcceptLoopFinished) -> Task<void> {
        if (lifecycle_ != Lifecycle::done) {
          lifecycle_ = Lifecycle::draining_connections;
        }
        maybe_finish_draining();
        co_return;
      },
      [&](ConnectionReleased) -> Task<void> {
        maybe_finish_draining();
        co_return;
      });
  }

  auto process_sub(SubKeyView, nova::Events events, Push<nova::Events>& push,
                   OpCtx&) -> Task<void> override {
    auto const rows = events.active_count();
    co_await push(std::move(events));
    events_read_counter_.add(rows);
  }

  auto finish_sub(SubKeyView key, Push<nova::Events>&, OpCtx& ctx)
    -> Task<void> override {
    auto conn_id = static_cast<uint64_t>(as<int64_t>(key));
    auto it = connections_.find(conn_id);
    if (it == connections_.end()) {
      co_return;
    }
    auto connection = std::move(it->second);
    connections_.erase(it);
    Impl::close_connection_state(connection.state);
    close_stream_transport(std::move(connection.transport));
    release_connection_slot();
    auto error = std::move(*co_await connection.read_error->lock());
    if (error) {
      impl_.emit_read_warning(conn_id, *error, ctx.dh());
    }
    maybe_finish_draining();
  }

  auto finish_sub(SubKeyView key, failure error, Push<nova::Events>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(error);
    co_await finish_sub(key, push, ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    // Restored subpipelines keep their keys while they drain, so connections
    // accepted after a restart must not reuse the IDs from before it.
    serde("next_conn_id", next_conn_id_);
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining_accept_loop;
      stop_accepting();
    }
    close_all_connections();
    maybe_finish_draining();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining_accept_loop;
      stop_accepting();
    }
    close_all_connections();
    maybe_finish_draining();
  }

private:
  enum class Lifecycle {
    running,
    draining_accept_loop,
    draining_connections,
    done,
  };

  struct ConnectionEntry {
    Connection transport;
    ConnectionState state;
    Arc<Mutex<Option<std::string>>> read_error;
  };

  auto stop_accepting() -> void {
    accept_cancel_->requestCancellation();
    impl_.stop_accepting(evb_);
  }

  auto request_abort() -> void {
    if (lifecycle_ == Lifecycle::done) {
      return;
    }
    lifecycle_ = Lifecycle::done;
    stop_accepting();
    close_all_connections();
  }

  auto close_all_connections() -> void {
    for (auto& [_, connection] : connections_) {
      Impl::close_connection_state(connection.state);
      close_stream_transport(connection.transport);
      release_connection_slot();
    }
    connections_.clear();
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ != Lifecycle::draining_connections) {
      return;
    }
    if (static_cast<uint64_t>(connection_slots_.available_permits())
        == max_connections_) {
      lifecycle_ = Lifecycle::done;
      impl_.cleanup();
    }
  }

  static auto sub_key_for(uint64_t conn_id) -> data {
    TENZIR_ASSERT(
      conn_id <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    return data{int64_t{static_cast<int64_t>(conn_id)}};
  }

  auto release_connection_slot() -> void {
    connection_slots_.add_permit();
  }

  auto finish_accept(Box<folly::coro::Transport> transport, OpCtx& ctx)
    -> Task<void> {
    // The main loop may be waiting for this slot to finish draining.
    auto release_connection_slot_guard
      = detail::scope_guard{[this, &ctx]() noexcept {
          release_connection_slot();
          ctx.spawn_task([this]() -> Task<void> {
            co_await message_queue_->enqueue(ConnectionReleased{});
          });
        }};
    auto current_token = co_await folly::coro::co_current_cancellation_token;
    auto local_token = accept_cancel_->getToken();
    auto cancel_token
      = folly::cancellation_token_merge(current_token, local_token);
    if (cancel_token.isCancellationRequested()) {
      close_stream_transport(std::move(transport));
      co_return;
    }
    auto accepted = co_await impl_.finish_accept(
      std::move(transport), current_token, local_token, cancel_token, ctx.dh());
    if (not accepted) {
      co_return;
    }
    if (cancel_token.isCancellationRequested()) {
      close_stream_transport(std::move(accepted->transport));
      co_return;
    }
    co_await message_queue_->enqueue(Accepted{std::move(*accepted)});
    release_connection_slot_guard.disable();
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_DEBUG("{}: accept loop started", impl_.debug_name());
    while (true) {
      warn_if_connection_limit_reached(ctx.dh());
      co_await connection_slots_.consume();
      auto release_connection_slot_guard
        = detail::scope_guard{[this]() noexcept {
            release_connection_slot();
          }};
      auto transport = co_await folly::coro::retryWithExponentialBackoff(
        std::numeric_limits<uint32_t>::max(), accept_retry_delay,
        accept_retry_delay, 0.0,
        [this, &ctx]() -> Task<Box<folly::coro::Transport>> {
          try {
            co_return co_await impl_.accept(evb_);
          } catch (folly::AsyncSocketException const& ex) {
            impl_.emit_accept_warning(ex, ctx.dh());
            throw;
          }
        },
        should_retry_socket);
      ctx.spawn_task(finish_accept(std::move(transport), ctx));
      release_connection_slot_guard.disable();
    }
  }

  /// Warns when all connection slots are taken, at most once per interval.
  /// New clients then wait in the listen backlog, and nothing else reports
  /// that they cannot connect.
  auto warn_if_connection_limit_reached(diagnostic_handler& dh) -> void {
    if (connection_slots_.available_permits() != 0) {
      return;
    }
    auto now = std::chrono::steady_clock::now();
    if (last_connection_limit_warning_
        and now - *last_connection_limit_warning_
              < connection_limit_warning_interval) {
      return;
    }
    last_connection_limit_warning_ = now;
    impl_.emit_connection_limit_warning(max_connections_, dh);
  }

  static constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
  static constexpr auto connection_limit_warning_interval
    = std::chrono::minutes{5};
  static constexpr auto message_queue_capacity = uint32_t{1'024};
  Option<std::chrono::steady_clock::time_point> last_connection_limit_warning_;
  Impl impl_;
  folly::EventBase* evb_ = nullptr;
  uint64_t max_connections_ = 128;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Semaphore connection_slots_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  /// Connections whose subpipelines still run and hold a connection slot.
  std::unordered_map<uint64_t, ConnectionEntry> connections_;
  MetricsCounter events_read_counter_;
  uint64_t next_conn_id_{0};
  Lifecycle lifecycle_ = Lifecycle::running;
};

} // namespace tenzir
