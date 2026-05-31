//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async/semaphore.hpp>
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

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>
#include <folly/io/coro/TransportCallbacks.h>

#include <filesystem>
#include <limits>
#include <memory>

namespace tenzir::plugins::accept_uds {

namespace {

using namespace tenzir::si_literals;

constexpr auto buffer_size = size_t{64_Ki};
constexpr auto listen_backlog = uint32_t{128};
constexpr auto message_queue_capacity = uint32_t{1_Ki};
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};

struct AcceptUdsArgs {
  located<std::string> path;
  Option<located<uint64_t>> max_connections;
  located<ir::pipeline> user_pipeline;
};

class AcceptUdsListener final : public Operator<void, table_slice> {
public:
  using Connection = Arc<folly::coro::Transport>;

  struct Accepted {
    Connection transport;
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

  explicit AcceptUdsListener(AcceptUdsArgs args)
    : args_{std::move(args)},
      max_connections_{args_.max_connections ? args_.max_connections->inner
                                             : uint64_t{128}},
      connection_slots_{detail::narrow<size_t>(max_connections_)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    path_ = expand_home(args_.path.inner);
    auto address = make_uds_socket_address(path_, args_.path.source, ctx.dh());
    if (not address) {
      request_abort();
      co_return;
    }
    if (not prepare_uds_listen_path(path_, args_.path.source, ctx.dh())) {
      request_abort();
      co_return;
    }
    address_ = std::move(*address);
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    server_.emplace(
      std::in_place,
      Arc<folly::AsyncServerSocket>::from_non_null(std::move(socket)), address_,
      listen_backlog);
    should_cleanup_socket_ = true;
    accept_loop_finished_ = false;
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_uds"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "accept_uds"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
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
        auto transport = std::move(accepted.transport);
        if (lifecycle_ != Lifecycle::running) {
          close_transport(std::move(transport));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        auto* transport_evb = transport->getEventBase();
        TENZIR_ASSERT(transport_evb);
        auto conn_id = next_conn_id_++;
        auto pipeline_copy = args_.user_pipeline.inner;
        if (not pipeline_copy.substitute(substitute_ctx{{ctx}, nullptr},
                                         true)) {
          close_transport(std::move(transport));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        auto key = sub_key_for(conn_id);
        co_await ctx.spawn_sub<chunk_ptr>(std::move(key),
                                          std::move(pipeline_copy),
                                          DiagnosticBehavior::ErrorToWarning);
        auto [_, inserted]
          = connections_.emplace(conn_id, std::move(transport));
        TENZIR_ASSERT(inserted);
        auto it = connections_.find(conn_id);
        if (it == connections_.end()) {
          co_return;
        }
        auto message_queue = message_queue_;
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb,
          read_loop(conn_id, it->second, std::move(message_queue),
                    bytes_read_counter_)));
      },
      [&](Payload payload) -> Task<void> {
        auto key = sub_key_for(payload.conn_id);
        if (auto sub = ctx.get_sub(make_view(key))) {
          auto& pipe = as<SubHandle<chunk_ptr>>(*sub);
          auto push_result = co_await pipe.push(std::move(payload.chunk));
          if (push_result.is_err()) {
            if (auto it = connections_.find(payload.conn_id);
                it != connections_.end()) {
              close_transport(it->second);
            }
          }
        }
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (closed.error) {
          diagnostic::warning("connection closed after read error")
            .primary(args_.path.source)
            .note("connection id: {}", closed.conn_id)
            .note("reason: {}", *closed.error)
            .emit(ctx);
        }
        if (connections_.erase(closed.conn_id) == 1) {
          co_await close_subpipeline(closed.conn_id, ctx);
          release_connection_slot();
        }
        maybe_finish_draining();
      },
      [&](AcceptLoopFinished) -> Task<void> {
        accept_loop_finished_ = true;
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
      auto connection = std::move(it->second);
      connections_.erase(it);
      release_connection_slot();
      close_transport(std::move(connection));
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
      lifecycle_ = Lifecycle::draining;
      stop_accepting();
      close_all_connections();
    }
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
    lifecycle_ = Lifecycle::draining;
    stop_accepting();
    close_all_connections();
    maybe_finish_draining();
  }

private:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  auto stop_accepting() -> void {
    accept_cancel_->requestCancellation();
    if (server_ and evb_) {
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
        if (server_) {
          (*server_)->close();
        }
      });
    }
    cleanup_socket_path();
  }

  auto cleanup_socket_path() -> void {
    if (not should_cleanup_socket_) {
      return;
    }
    should_cleanup_socket_ = false;
    auto ec = std::error_code{};
    std::filesystem::remove(path_, ec);
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
      close_transport(connection);
      release_connection_slot();
    }
    connections_.clear();
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ != Lifecycle::draining) {
      return;
    }
    if (accept_loop_finished_
        and static_cast<uint64_t>(connection_slots_.available_permits())
              == max_connections_) {
      lifecycle_ = Lifecycle::done;
      cleanup_socket_path();
    }
  }

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

  auto finish_accept(Box<folly::coro::Transport> transport) -> Task<void> {
    auto release_connection_slot_guard = detail::scope_guard{[this]() noexcept {
      release_connection_slot();
    }};
    auto current_token = co_await folly::coro::co_current_cancellation_token;
    auto cancel_token = folly::cancellation_token_merge(
      current_token, accept_cancel_->getToken());
    if (lifecycle_ != Lifecycle::running
        or cancel_token.isCancellationRequested()) {
      close_transport(std::move(transport));
      co_return;
    }
    TENZIR_DEBUG("accept_uds: accepted connection on {}", path_);
    co_await message_queue_->enqueue(Accepted{
      .transport = Connection{std::move(*transport)},
    });
    release_connection_slot_guard.disable();
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_DEBUG("accept_uds: accept loop started on {}", path_);
    auto should_retry_accept = [](folly::exception_wrapper const& ew) {
      return ew.is_compatible_with<folly::AsyncSocketException>();
    };
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
            co_return co_await folly::coro::co_withExecutor(
              evb_, (*server_)->accept());
          } catch (folly::AsyncSocketException const& ex) {
            diagnostic::warning("failed to accept incoming connection")
              .primary(args_.path.source)
              .note("path: {}", path_)
              .note("reason: {}", ex.what())
              .emit(ctx.dh());
            throw;
          }
        },
        should_retry_accept);
      ctx.spawn_task(finish_accept(std::move(transport)));
      release_connection_slot_guard.disable();
    }
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

  AcceptUdsArgs args_;
  std::string path_;
  folly::SocketAddress address_;
  folly::EventBase* evb_ = nullptr;
  Option<Box<UdsServerSocket>> server_;
  uint64_t max_connections_ = 128;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Semaphore connection_slots_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  std::unordered_map<uint64_t, Connection> connections_;
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
  uint64_t next_conn_id_{0};
  bool accept_loop_finished_ = true;
  bool should_cleanup_socket_ = false;
  Lifecycle lifecycle_ = Lifecycle::running;
};

class AcceptUdsPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.accept_uds";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptUdsArgs, AcceptUdsListener>{};
    d.positional("path", &AcceptUdsArgs::path);
    auto max_connections_arg
      = d.named("max_connections", &AcceptUdsArgs::max_connections);
    auto pipeline_arg
      = d.pipeline(&AcceptUdsArgs::user_pipeline, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto max_connections = ctx.get(max_connections_arg);
          max_connections) {
        auto loc
          = ctx.get_location(max_connections_arg).value_or(location::unknown);
        if (max_connections->inner == 0) {
          diagnostic::error("max_connections must be greater than 0")
            .primary(loc)
            .emit(ctx);
        } else if (max_connections->inner > static_cast<uint64_t>(
                     std::numeric_limits<size_t>::max())) {
          diagnostic::error("max_connections is too large")
            .primary(loc)
            .note("maximum supported value: {}",
                  std::numeric_limits<size_t>::max())
            .emit(ctx);
        }
      }
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

} // namespace tenzir::plugins::accept_uds

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_uds::AcceptUdsPlugin)
