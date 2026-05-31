//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "detail/stream.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/async/stream.hpp>
#include <tenzir/async/uds.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/file.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>
#include <memory>

namespace tenzir::plugins::from_uds {

namespace {

using namespace tenzir::si_literals;

constexpr auto buffer_size = size_t{64_Ki};
constexpr auto connect_max_retries
  = stream_detail::default_connect_max_retry_count;
constexpr auto message_queue_capacity = uint32_t{1_Ki};

struct FromUdsArgs {
  located<std::string> path;
  located<ir::pipeline> user_pipeline;
};

class FromUdsConnector final : public Operator<void, table_slice> {
public:
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

  explicit FromUdsConnector(FromUdsArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    path_ = expand_home(args_.path.inner);
    auto address = make_uds_socket_address(path_, args_.path.source, ctx.dh());
    if (not address) {
      startup_failed_ = true;
      co_return;
    }
    address_ = std::move(*address);
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_uds"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    if (current_connection_) {
      co_return co_await message_queue_->dequeue();
    }
    auto transport = co_await folly::coro::retryWithExponentialBackoff(
      connect_max_retries, stream_detail::connect_initial_backoff,
      stream_detail::connect_max_backoff, stream_detail::connect_retry_jitter,
      [this, &dh]() -> Task<Box<folly::coro::Transport>> {
        TENZIR_DEBUG("from_uds: connecting to {}", path_);
        try {
          co_return Box<folly::coro::Transport>{
            co_await folly::coro::co_withExecutor(
              evb_, folly::coro::Transport::newConnectedSocket(
                      evb_, address_,
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                        stream_detail::connect_timeout)))};
        } catch (folly::AsyncSocketException const& ex) {
          diagnostic::warning("failed to connect to UNIX domain socket")
            .primary(args_.path.source)
            .note("path: {}", path_)
            .note("reason: {}", stream_detail::describe_socket_error(ex))
            .hint("ensure a server is listening on this socket path")
            .emit(dh);
          throw;
        }
      },
      stream_detail::should_retry_socket);
    TENZIR_DEBUG("from_uds: connected to {}", path_);
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
        auto pipeline_copy = args_.user_pipeline.inner;
        if (not pipeline_copy.substitute(substitute_ctx{{ctx}, nullptr},
                                         true)) {
          stream_detail::close_transport(std::move(transport));
          co_return;
        }
        co_await ctx.spawn_sub<chunk_ptr>(data{int64_t(conn_id)},
                                          std::move(pipeline_copy));
        current_conn_id_ = conn_id;
        current_connection_ = Connection{std::move(*transport)};
        bytes_read_counter_
          = ctx.make_counter(MetricsLabel{"operator", "from_uds"},
                             MetricsDirection::read,
                             MetricsVisibility::external_, MetricsUnit::bytes);
        auto message_queue = message_queue_;
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb,
          read_loop(conn_id, *current_connection_, std::move(message_queue),
                    bytes_read_counter_)));
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
          diagnostic::warning("connection closed after read error")
            .primary(args_.path.source)
            .note("path: {}", path_)
            .note("reason: {}", *closed.error)
            .emit(ctx);
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
        stream_detail::close_transport(std::move(connection));
      }
      current_conn_id_ = None{};
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return startup_failed_ ? OperatorState::done : OperatorState::normal;
  }

private:
  static auto read_loop(uint64_t conn_id, Connection connection,
                        Arc<MessageQueue> message_queue,
                        MetricsCounter bytes_read_counter) -> Task<void> {
    co_await stream_detail::read_loop<Payload, ConnectionClosed>(
      conn_id, std::move(connection), std::move(message_queue), buffer_size,
      std::move(bytes_read_counter), [](size_t) {});
  }

  FromUdsArgs args_;
  std::string path_;
  folly::SocketAddress address_;
  folly::EventBase* evb_ = nullptr;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Connection> current_connection_;
  Option<uint64_t> current_conn_id_;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  uint64_t next_conn_id_{0};
  bool startup_failed_ = false;
};

class FromUdsPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_uds";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromUdsArgs, FromUdsConnector>{};
    d.positional("path", &FromUdsArgs::path);
    auto pipeline_arg
      = d.pipeline(&FromUdsArgs::user_pipeline, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto pipeline, ctx.get(pipeline_arg));
      auto output = pipeline.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(pipeline.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_uds

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_uds::FromUdsPlugin)
