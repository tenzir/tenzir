//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async/uds.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
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
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>
#include <folly/io/coro/TransportCallbacks.h>

#include <limits>
#include <memory>

namespace tenzir::plugins::from_uds {

namespace {

using namespace tenzir::si_literals;

constexpr auto buffer_size = size_t{64_Ki};
constexpr auto connect_timeout = std::chrono::seconds{5};
constexpr auto connect_initial_backoff = std::chrono::milliseconds{100};
constexpr auto connect_max_backoff = std::chrono::milliseconds{5_k};
constexpr auto connect_retry_jitter = 0.0;
constexpr auto connect_max_retries = std::numeric_limits<uint32_t>::max();
constexpr auto message_queue_capacity = uint32_t{1_Ki};

constexpr auto should_retry_connect = [](folly::exception_wrapper const& ew) {
  return ew.is_compatible_with<folly::AsyncSocketException>();
};

struct FromUdsArgs {
  located<std::string> path;
  located<ir::pipeline> user_pipeline;
};

auto describe_socket_error(folly::AsyncSocketException const& ex)
  -> std::string {
  if (auto err = ex.getErrno(); err > 0) {
    return folly::errnoStr(err);
  }
  return ex.what();
}

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
      connect_max_retries, connect_initial_backoff, connect_max_backoff,
      connect_retry_jitter,
      [this, &dh]() -> Task<Box<folly::coro::Transport>> {
        TENZIR_DEBUG("from_uds: connecting to {}", path_);
        try {
          TENZIR_ASSERT(address_);
          co_return Box<folly::coro::Transport>{
            co_await folly::coro::co_withExecutor(
              evb_, folly::coro::Transport::newConnectedSocket(
                      evb_, *address_,
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                        connect_timeout)))};
        } catch (folly::AsyncSocketException const& ex) {
          diagnostic::warning("failed to connect to UNIX domain socket")
            .primary(args_.path.source)
            .note("path: {}", path_)
            .note("reason: {}", describe_socket_error(ex))
            .hint("ensure a server is listening on this socket path")
            .emit(dh);
          throw;
        }
      },
      should_retry_connect);
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
          close_transport(std::move(transport));
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
        close_transport(std::move(connection));
      }
      current_conn_id_ = None{};
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return address_ ? OperatorState::normal : OperatorState::done;
  }

private:
  static auto close_transport(Connection connection) -> void {
    auto* evb = connection->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([connection = std::move(connection)]() mutable {
      connection->close();
    });
  }

  static auto close_transport(Box<folly::coro::Transport> transport) -> void {
    auto* evb = transport->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([transport = std::move(transport)]() mutable {
      transport->close();
    });
  }

  static auto read_loop(uint64_t conn_id, Connection connection,
                        Arc<MessageQueue> message_queue,
                        MetricsCounter bytes_read_counter) -> Task<void> {
    auto read_error = std::string{};
    while (true) {
      try {
        auto* evb = connection->getEventBase();
        TENZIR_ASSERT(evb);
        auto* async_transport = connection->getTransport();
        TENZIR_ASSERT(async_transport);
        auto buffer = folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};
        auto callback = folly::coro::ReadCallback{
          evb->timer(), *async_transport,
          &buffer,      1,
          buffer_size,  std::chrono::milliseconds{0},
        };
        async_transport->setReadCB(&callback);
        auto reset_read_callback
          = detail::scope_guard{[async_transport]() noexcept {
              async_transport->setReadCB(nullptr);
            }};
        co_await callback.wait();
        if (callback.error()) {
          callback.error().throw_exception();
        }
        if (buffer.chainLength() == 0) {
          break;
        }
        auto iobuf = buffer.move();
        auto range = iobuf->coalesce();
        auto chunk = chunk::make(as_bytes(range.data(), range.size()),
                                 [buf = std::move(iobuf)]() noexcept {
                                   static_cast<void>(buf);
                                 });
        bytes_read_counter.add(chunk->size());
        co_await message_queue->enqueue(Payload{conn_id, std::move(chunk)});
      } catch (folly::AsyncSocketException const& e) {
        read_error = e.what();
        break;
      }
    }
    co_await message_queue->enqueue(ConnectionClosed{
      conn_id,
      read_error.empty() ? Option<std::string>{}
                         : Option<std::string>{std::move(read_error)},
    });
  }

  FromUdsArgs args_;
  std::string path_;
  // Empty when startup failed, stopping before `await_task()` connects.
  Option<folly::SocketAddress> address_;
  folly::EventBase* evb_ = nullptr;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Connection> current_connection_;
  Option<uint64_t> current_conn_id_;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  uint64_t next_conn_id_{0};
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
