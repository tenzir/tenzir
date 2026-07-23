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
#include "tenzir/option.hpp"
#include "tenzir/pipeline_metrics.hpp"

#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>

namespace tenzir {

template <class Impl>
class StreamFrom final : public Operator<void, table_slice> {
public:
  using Args = typename Impl::Args;
  using ConnectionInfo = typename Impl::ConnectionInfo;
  using Connection = Arc<folly::coro::Transport>;

  struct Connected {
    Box<folly::coro::Transport> transport;
  };

  struct Payload {
    uint64_t conn_id;
    chunk_ptr chunk;
  };

  struct ConnectionClosed {
    uint64_t conn_id;
    Option<std::string> error;
  };

  using Message = variant<Connected, Payload, ConnectionClosed>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  explicit StreamFrom(Args args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await impl_.prepare(ctx)) {
      co_return;
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    events_read_counter_
      = ctx.make_counter(impl_.events_metric_label(), MetricsDirection::read,
                         MetricsVisibility::external_, MetricsUnit::events);
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    if (current_connection_) {
      co_return co_await message_queue_->dequeue();
    }
    auto transport = co_await folly::coro::retryWithExponentialBackoff(
      connect_max_retries, connect_initial_backoff, connect_max_backoff,
      connect_retry_jitter,
      [this, &dh]() -> Task<Box<folly::coro::Transport>> {
        try {
          co_return co_await impl_.connect(evb_);
        } catch (folly::AsyncSocketException const& ex) {
          impl_.emit_connect_warning(ex, dh);
          throw;
        }
      },
      should_retry_socket);
    co_return Message{Connected{std::move(transport)}};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto* message = result.try_as<Message>();
    if (not message) {
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](Connected connected) -> Task<void> {
        auto transport = std::move(connected.transport);
        auto* transport_evb = transport->getEventBase();
        TENZIR_ASSERT(transport_evb);
        auto conn_id = next_conn_id_++;
        auto info = impl_.make_connection_info(*transport, ctx);
        auto pipeline_copy = impl_.pipeline().inner;
        impl_.bind(pipeline_copy, info, ctx);
        co_await ctx.spawn_sub<chunk_ptr>(data{int64_t(conn_id)},
                                          std::move(pipeline_copy));
        current_conn_id_ = conn_id;
        current_connection_ = Connection{std::move(*transport)};
        auto bytes_read_counter
          = ctx.make_counter(impl_.bytes_metric_label(info),
                             MetricsDirection::read,
                             MetricsVisibility::external_, MetricsUnit::bytes);
        auto message_queue = message_queue_;
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb,
          read_loop(conn_id, *current_connection_, std::move(message_queue),
                    std::move(bytes_read_counter))));
      },
      [&](Payload payload) -> Task<void> {
        if (not current_conn_id_ or payload.conn_id != *current_conn_id_) {
          co_return;
        }
        auto sub_key = data{int64_t(*current_conn_id_)};
        if (auto sub = ctx.get_sub(make_view(sub_key))) {
          auto push_result = co_await as<SubHandle<chunk_ptr>>(*sub).push(
            std::move(payload.chunk));
          TENZIR_UNUSED(push_result);
        }
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (closed.error) {
          impl_.emit_read_warning(*closed.error, ctx.dh());
        }
        if (current_conn_id_ and *current_conn_id_ == closed.conn_id) {
          current_connection_ = None{};
          current_conn_id_ = None{};
          auto closed_key = data{int64_t(closed.conn_id)};
          if (auto sub = ctx.get_sub(make_view(closed_key))) {
            co_await as<SubHandle<chunk_ptr>>(*sub).close();
          }
        }
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
    if (current_conn_id_ and *current_conn_id_ == conn_id) {
      if (current_connection_) {
        auto connection = std::move(*current_connection_);
        current_connection_ = None{};
        close_stream_transport(std::move(connection));
      }
      current_conn_id_ = None{};
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return impl_.ready() ? OperatorState::normal : OperatorState::done;
  }

private:
  static constexpr auto buffer_size = size_t{64 * 1024};
  static constexpr auto connect_initial_backoff
    = std::chrono::milliseconds{100};
  static constexpr auto connect_max_backoff = std::chrono::milliseconds{5'000};
  static constexpr auto connect_retry_jitter = 0.0;
  static constexpr auto connect_max_retries
    = std::numeric_limits<uint32_t>::max();
  static constexpr auto message_queue_capacity = uint32_t{1'024};

  static auto read_loop(uint64_t conn_id, Connection connection,
                        Arc<MessageQueue> message_queue,
                        MetricsCounter bytes_read_counter) -> Task<void> {
    auto read_error = Option<std::string>{};
    while (true) {
      try {
        auto read_result = co_await read_stream_chunk(
          *connection, buffer_size, std::chrono::milliseconds{0});
        if (not read_result) {
          break;
        }
        bytes_read_counter.add((*read_result)->size());
        co_await message_queue->enqueue(
          Payload{conn_id, std::move(*read_result)});
      } catch (folly::AsyncSocketException const& e) {
        read_error = e.what();
        break;
      }
    }
    co_await message_queue->enqueue(
      ConnectionClosed{conn_id, std::move(read_error)});
  }

  Impl impl_;
  folly::EventBase* evb_ = nullptr;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Connection> current_connection_;
  Option<uint64_t> current_conn_id_;
  MetricsCounter events_read_counter_;
  uint64_t next_conn_id_{0};
};

} // namespace tenzir
