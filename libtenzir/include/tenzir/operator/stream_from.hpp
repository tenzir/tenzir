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
#include "tenzir/async/stream.hpp"
#include "tenzir/box.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline_metrics.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/coro/WithCancellation.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>

namespace tenzir {

template <class Impl>
class StreamFrom final : public Operator<void, nova::Events> {
public:
  using Args = typename Impl::Args;
  using ConnectionInfo = typename Impl::ConnectionInfo;
  using Connection = Arc<folly::coro::Transport>;

  struct Connected {
    uint64_t conn_id;
    Connection transport;
    ConnectionInfo info;
  };

  struct Payload {
    uint64_t conn_id;
    chunk_ptr chunk;
  };

  struct ConnectionClosed {
    uint64_t conn_id;
    Option<std::string> error;
  };

  struct ConnectionLoopFinished {};

  using Message
    = variant<ConnectionLoopFinished, Connected, Payload, ConnectionClosed>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  explicit StreamFrom(Args args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await impl_.prepare(ctx)) {
      done_ = true;
      co_return;
    }
    events_read_counter_
      = ctx.make_counter(impl_.events_metric_label(), MetricsDirection::read,
                         MetricsVisibility::external_, MetricsUnit::events);
    ctx.spawn_task([this, &ctx]() -> Task<void> {
      auto notify_finished = detail::scope_guard{[this, &ctx]() noexcept {
        ctx.spawn_task([this]() -> Task<void> {
          co_await message_queue_->enqueue(ConnectionLoopFinished{});
        });
      }};
      co_await connection_loop(ctx);
    });
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto* message = result.try_as<Message>();
    if (not message) {
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](Connected connected) -> Task<void> {
        auto pipeline = impl_.pipeline().inner;
        impl_.bind(pipeline, connected.info, ctx);
        if (not co_await ctx.plan_and_spawn_sub<chunk_ptr>(
              sub_key(connected.conn_id), std::move(pipeline))) {
          close_stream_transport(std::move(connected.transport));
          co_return;
        }
        current_ = CurrentConnection{
          .conn_id = connected.conn_id,
          .transport = std::move(connected.transport),
        };
      },
      [&](Payload payload) -> Task<void> {
        if (auto sub = ctx.get_sub(make_view(sub_key(payload.conn_id)))) {
          auto push_result = co_await as<SubHandle<chunk_ptr>>(*sub).push(
            std::move(payload.chunk));
          TENZIR_UNUSED(push_result);
        }
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (closed.error) {
          impl_.emit_read_warning(*closed.error, ctx.dh());
        }
        if (current_ and current_->conn_id == closed.conn_id) {
          current_ = None{};
        }
        if (auto sub = ctx.get_sub(make_view(sub_key(closed.conn_id)))) {
          co_await as<SubHandle<chunk_ptr>>(*sub).close();
        }
      },
      [&](ConnectionLoopFinished) -> Task<void> {
        done_ = true;
        co_return;
      });
  }

  auto process_sub(SubKeyView, nova::Events events, Push<nova::Events>& push,
                   OpCtx&) -> Task<void> override {
    auto const rows = events.active_count();
    co_await push(std::move(events));
    events_read_counter_.add(rows);
  }

  auto finish_sub(SubKeyView key, Push<nova::Events>&, OpCtx&)
    -> Task<void> override {
    if (current_ and sub_key(current_->conn_id) == key) {
      close_stream_transport(std::move(current_->transport));
      current_ = None{};
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    stop_->requestCancellation();
    co_return;
  }

private:
  struct CurrentConnection {
    uint64_t conn_id;
    Connection transport;
  };

  static constexpr auto buffer_size = size_t{64 * 1024};
  static constexpr auto connect_initial_backoff
    = std::chrono::milliseconds{100};
  static constexpr auto connect_max_backoff = std::chrono::milliseconds{5'000};
  static constexpr auto connect_retry_jitter = 0.0;
  static constexpr auto connect_max_retries
    = std::numeric_limits<uint32_t>::max();
  static constexpr auto message_queue_capacity = uint32_t{1'024};

  static auto sub_key(uint64_t conn_id) -> data {
    return data{static_cast<int64_t>(conn_id)};
  }

  auto connection_loop(OpCtx& ctx) -> Task<void> {
    auto io_executor = ctx.io_executor();
    auto* evb = io_executor->getEventBase();
    auto stop = folly::cancellation_token_merge(
      co_await folly::coro::co_current_cancellation_token, stop_->getToken());
    for (auto conn_id = uint64_t{0};; ++conn_id) {
      auto connected
        = co_await folly::coro::co_withCancellation(stop, connect(evb, ctx));
      auto transport = Connection{std::move(*connected)};
      auto close_transport = detail::scope_guard{[&]() noexcept {
        close_stream_transport(transport);
      }};
      auto info = impl_.make_connection_info(*transport, ctx);
      auto bytes_read_counter
        = ctx.make_counter(impl_.bytes_metric_label(info),
                           MetricsDirection::read, MetricsVisibility::external_,
                           MetricsUnit::bytes);
      co_await message_queue_->enqueue(
        Connected{conn_id, transport, std::move(info)});
      auto error = co_await folly::coro::co_withExecutor(
        evb, read_all(conn_id, *transport, bytes_read_counter, stop));
      co_await message_queue_->enqueue(
        ConnectionClosed{conn_id, std::move(error)});
    }
  }

  auto connect(folly::EventBase* evb, OpCtx& ctx)
    -> Task<Box<folly::coro::Transport>> {
    // The first attempt reaches the network before it checks for cancellation.
    co_await folly::coro::co_safe_point;
    co_return co_await folly::coro::retryWithExponentialBackoff(
      connect_max_retries, connect_initial_backoff, connect_max_backoff,
      connect_retry_jitter,
      [this, evb, &ctx]() -> Task<Box<folly::coro::Transport>> {
        try {
          co_return co_await impl_.connect(evb);
        } catch (folly::AsyncSocketException const& ex) {
          impl_.emit_connect_warning(ex, ctx.dh());
          throw;
        }
      },
      should_retry_socket);
  }

  auto read_all(uint64_t conn_id, folly::coro::Transport& transport,
                MetricsCounter& bytes_read_counter,
                folly::CancellationToken stop) const
    -> Task<Option<std::string>> {
    try {
      // Only reading observes `stop`, so that read data still gets delivered.
      while (auto chunk = co_await folly::coro::co_withCancellation(
               stop, read_stream_chunk(transport, buffer_size,
                                       std::chrono::milliseconds{0}))) {
        bytes_read_counter.add((*chunk)->size());
        co_await message_queue_->enqueue(Payload{conn_id, std::move(*chunk)});
      }
    } catch (folly::AsyncSocketException const& ex) {
      co_return ex.what();
    }
    co_return None{};
  }

  Impl impl_;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Box<folly::CancellationSource> stop_{std::in_place};
  Option<CurrentConnection> current_;
  MetricsCounter events_read_counter_;
  bool done_ = false;
};

} // namespace tenzir
