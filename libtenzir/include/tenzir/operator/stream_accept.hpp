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
#include "tenzir/async/semaphore.hpp"
#include "tenzir/async/stream.hpp"
#include "tenzir/box.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline_metrics.hpp"

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>
#include <unordered_map>

namespace tenzir {

template <class Impl>
class StreamAccept final : public Operator<void, table_slice> {
public:
  using Args = typename Impl::Args;
  using AcceptedInfo = typename Impl::AcceptedInfo;
  using ConnectionState = typename Impl::ConnectionState;
  using Connection = Arc<folly::coro::Transport>;

  struct Accepted {
    AcceptedInfo info;
  };

  struct Payload {
    uint64_t conn_id;
    chunk_ptr chunk;
  };

  struct ConnectionClosed {
    uint64_t conn_id;
    Option<std::string> error;
  };

  struct AcceptLoopFinished {};

  using Message
    = variant<AcceptLoopFinished, Accepted, Payload, ConnectionClosed>;
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

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
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
        auto* transport_evb = transport->getEventBase();
        TENZIR_ASSERT(transport_evb);
        auto conn_id = next_conn_id_++;
        auto pipeline_copy = impl_.pipeline().inner;
        if (not impl_.substitute(pipeline_copy, accepted.info, ctx)) {
          close_stream_transport(std::move(transport));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        auto key = sub_key_for(conn_id);
        co_await ctx.spawn_sub<chunk_ptr>(std::move(key),
                                          std::move(pipeline_copy),
                                          DiagnosticBehavior::ErrorToWarning);
        auto state
          = impl_.make_connection_state(*transport, accepted.info, ctx);
        auto bytes_read_counter
          = ctx.make_counter(impl_.bytes_metric_label(accepted.info),
                             MetricsDirection::read,
                             MetricsVisibility::external_, MetricsUnit::bytes);
        auto [_, inserted] = connections_.emplace(
          conn_id, ConnectionEntry{std::move(transport), std::move(state)});
        TENZIR_ASSERT(inserted);
        auto it = connections_.find(conn_id);
        if (it == connections_.end()) {
          co_return;
        }
        auto message_queue = message_queue_;
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb,
          read_loop(conn_id, it->second.transport, std::move(message_queue),
                    std::move(bytes_read_counter), it->second.state)));
      },
      [&](Payload payload) -> Task<void> {
        auto key = sub_key_for(payload.conn_id);
        if (auto sub = ctx.get_sub(make_view(key))) {
          auto& pipe = as<SubHandle<chunk_ptr>>(*sub);
          auto push_result = co_await pipe.push(std::move(payload.chunk));
          if (push_result.is_err()) {
            if (auto it = connections_.find(payload.conn_id);
                it != connections_.end()) {
              close_stream_transport(it->second.transport);
            }
          }
        }
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (closed.error) {
          impl_.emit_read_warning(closed.conn_id, *closed.error, ctx.dh());
        }
        if (auto it = connections_.find(closed.conn_id);
            it != connections_.end()) {
          connections_.erase(it);
          co_await close_subpipeline(closed.conn_id, ctx);
          release_connection_slot();
        }
        maybe_finish_draining();
        co_return;
      },
      [&](AcceptLoopFinished) -> Task<void> {
        if (lifecycle_ != Lifecycle::done) {
          lifecycle_ = Lifecycle::draining_connections;
        }
        maybe_finish_draining();
        co_return;
      });
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx&) -> Task<void> override {
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    events_read_counter_.add(rows);
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto conn_id = static_cast<uint64_t>(as<int64_t>(key));
    if (auto it = connections_.find(conn_id); it != connections_.end()) {
      auto connection = std::move(it->second.transport);
      Impl::close_connection_state(it->second.state);
      connections_.erase(it);
      release_connection_slot();
      close_stream_transport(std::move(connection));
      maybe_finish_draining();
    }
    co_return;
  }

  auto finish_sub(SubKeyView key, failure error, Push<table_slice>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(error);
    co_await finish_sub(key, push, ctx);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
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

  static auto close_subpipeline(uint64_t conn_id, OpCtx& ctx) -> Task<void> {
    auto key = sub_key_for(conn_id);
    if (auto sub = ctx.get_sub(make_view(key))) {
      auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
      co_await pipeline.close();
    }
    co_return;
  }

  auto release_connection_slot() -> void {
    connection_slots_.add_permit();
  }

  auto finish_accept(Box<folly::coro::Transport> transport,
                     diagnostic_handler& dh) -> Task<void> {
    auto release_connection_slot_guard = detail::scope_guard{[this]() noexcept {
      release_connection_slot();
    }};
    auto current_token = co_await folly::coro::co_current_cancellation_token;
    auto local_token = accept_cancel_->getToken();
    auto cancel_token
      = folly::cancellation_token_merge(current_token, local_token);
    if (lifecycle_ != Lifecycle::running
        or cancel_token.isCancellationRequested()) {
      close_stream_transport(std::move(transport));
      co_return;
    }
    auto accepted = co_await impl_.finish_accept(
      std::move(transport), current_token, local_token, cancel_token, dh);
    if (not accepted) {
      co_return;
    }
    if (lifecycle_ != Lifecycle::running
        or cancel_token.isCancellationRequested()) {
      close_stream_transport(std::move(accepted->transport));
      co_return;
    }
    co_await message_queue_->enqueue(Accepted{std::move(*accepted)});
    release_connection_slot_guard.disable();
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_DEBUG("{}: accept loop started", impl_.debug_name());
    while (true) {
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
      ctx.spawn_task(finish_accept(std::move(transport), ctx.dh()));
      release_connection_slot_guard.disable();
    }
  }

  static auto
  read_loop(uint64_t conn_id, Connection connection,
            Arc<MessageQueue> message_queue, MetricsCounter bytes_read_counter,
            ConnectionState state) -> Task<void> {
    auto read_error = Option<std::string>{};
    while (true) {
      try {
        auto read_result = co_await read_stream_chunk(
          *connection, buffer_size, std::chrono::milliseconds{0});
        if (not read_result) {
          break;
        }
        bytes_read_counter.add((*read_result)->size());
        Impl::record_read(state, (*read_result)->size());
        co_await message_queue->enqueue(
          Payload{conn_id, std::move(*read_result)});
      } catch (folly::AsyncSocketException const& e) {
        read_error = e.what();
        break;
      }
    }
    Impl::close_connection_state(state);
    co_await message_queue->enqueue(
      ConnectionClosed{conn_id, std::move(read_error)});
  }

  static constexpr auto buffer_size = size_t{64 * 1024};
  static constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
  static constexpr auto message_queue_capacity = uint32_t{1'024};
  Impl impl_;
  folly::EventBase* evb_ = nullptr;
  uint64_t max_connections_ = 128;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Semaphore connection_slots_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  std::unordered_map<uint64_t, ConnectionEntry> connections_;
  MetricsCounter events_read_counter_;
  uint64_t next_conn_id_{0};
  Lifecycle lifecycle_ = Lifecycle::running;
};

} // namespace tenzir
