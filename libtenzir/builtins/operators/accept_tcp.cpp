//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/async/tcp.hpp>
#include <tenzir/async/tls.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/fibers/Semaphore.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <limits>
#include <memory>
#include <unordered_map>

namespace tenzir::plugins::accept_tcp {

namespace {

using namespace tenzir::si_literals;

// Read at most 64 KiB per socket callback. This keeps the per-connection
// working set modest while still amortizing callback overhead well for TCP
// streams, and it aligns with the movable-buffer read path below where folly
// hands us owned buffers that we transfer directly into chunks.
constexpr auto buffer_size = size_t{64_Ki};
// Match a typical TCP server listen queue depth: large enough for short bursts
// of incoming connections without implying that we expect unbounded fan-in.
constexpr auto listen_backlog = uint32_t{128};
// Leave room for many in-flight connection lifecycle notifications without
// turning the queue into another large per-listener memory sink.
constexpr auto message_queue_capacity = uint32_t{1_Ki};
// Retry accepts quickly after transient socket errors so the listener recovers
// fast, while still backing off enough to avoid a tight warning loop.
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};

struct AcceptTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  std::optional<located<uint64_t>> max_connections;
  located<ir::pipeline> user_pipeline;
  let_id peer_info;
};

class AcceptTcpListener final : public Operator<void, table_slice> {
public:
  using Connection = Arc<folly::coro::Transport>;

  struct Accepted {
    Box<folly::coro::Transport> transport;
  };

  struct ConnectionClosed {
    uint64_t conn_id;
    Option<std::string> error;
  };

  struct AcceptLoopFinished {};

  using Message = variant<Accepted, ConnectionClosed, AcceptLoopFinished>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  explicit AcceptTcpListener(AcceptTcpArgs args)
    : args_{std::move(args)},
      max_connections_{args_.max_connections ? args_.max_connections->inner
                                             : uint64_t{128}},
      connection_slots_{detail::narrow<size_t>(max_connections_)} {
    // Parse endpoint string to SocketAddress (validation already done in
    // describe)
    auto ep = to<struct endpoint>(args_.endpoint.inner);
    TENZIR_ASSERT(ep);
    TENZIR_ASSERT(ep->port);
    if (ep->host.empty()) {
      address_.setFromLocalPort(ep->port->number());
    } else {
      address_.setFromHostPort(ep->host, ep->port->number());
    }
    if (args_.tls) {
      tls_ = tls_options{*args_.tls, {.is_server = true}};
    }
  }

  AcceptTcpListener(AcceptTcpListener const&) = delete;
  auto operator=(AcceptTcpListener const&) -> AcceptTcpListener& = delete;
  AcceptTcpListener(AcceptTcpListener&&) noexcept = default;
  auto operator=(AcceptTcpListener&&) noexcept -> AcceptTcpListener& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    if (tls_ and tls_->get_tls(nullptr).inner) {
      auto context = tls_->make_folly_ssl_context(ctx);
      if (not context) {
        request_abort();
        co_return;
      }
      tls_context_ = std::move(*context);
    }
    TENZIR_DEBUG("starting listener on {}", address_.describe());
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    // Let ServerSocket handle bind/listen setup
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, listen_backlog);
    accept_loop_finished_ = false;
    ctx.spawn_task([this, &ctx]() -> Task<void> {
      auto notify_finished = detail::scope_guard{[this, &ctx]() noexcept {
        ctx.spawn_task([this]() -> Task<void> {
          co_await message_queue_->enqueue(AcceptLoopFinished{});
        });
      }};
      co_await folly::coro::co_withCancellation(accept_cancel_->getToken(),
                                                accept_loop(ctx));
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
        auto peer_addr = transport->getPeerAddress();
        auto peer_record = record{
          {"ip", peer_addr.getAddressStr()},
          {"port", int64_t{peer_addr.getPort()}},
        };
        auto conn_id = next_conn_id_++;
        auto pipeline_copy = args_.user_pipeline.inner;
        auto env = substitute_ctx::env_t{};
        env[args_.peer_info] = std::move(peer_record);
        auto bytes_read = ctx.make_counter(
          MetricsLabel{"peer_ip", MetricsLabel::FixedString::truncate(
                                    peer_addr.getAddressStr())},
          MetricsDirection::read, MetricsVisibility::external_);
        auto reg = global_registry();
        auto b_ctx = base_ctx{ctx, *reg};
        auto sub_result
          = pipeline_copy.substitute(substitute_ctx{b_ctx, &env}, true);
        if (not sub_result) {
          close_transport(std::move(transport));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        auto key = sub_key_for(conn_id);
        auto sub = co_await ctx.spawn_sub(
          std::move(key), std::move(pipeline_copy), tag_v<chunk_ptr>);
        auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
        auto connection = Connection{std::move(*transport)};
        auto [_, inserted]
          = connections_.emplace(conn_id, std::move(connection));
        TENZIR_ASSERT(inserted);
        auto message_queue = message_queue_;
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb,
          read_loop(conn_id, connections_.at(conn_id), std::move(open_pipeline),
                    std::move(message_queue), std::move(bytes_read))));
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (closed.error) {
          // TODO: Surface routine TCP read failures and disconnects as metrics
          // in a follow-up that covers all TCP operators.
          diagnostic::warning("connection closed after read error")
            .primary(args_.endpoint.source)
            .note("connection id: {}", closed.conn_id)
            .note("reason: {}", *closed.error)
            .emit(ctx);
        }
        if (connections_.erase(closed.conn_id) == 1) {
          co_await close_subpipeline(closed.conn_id, ctx);
          release_connection_slot();
        }
        maybe_finish_draining();
        co_return;
      },
      [&](AcceptLoopFinished) -> Task<void> {
        accept_loop_finished_ = true;
        maybe_finish_draining();
        co_return;
      });
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
                                         : OperatorState::unspecified;
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
          server_->close();
        }
      });
    }
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
    // All connection permits must be returned *and* the accept loop must have
    // finished before we can safely transition to done.
    // TODO: This is likely
    if (accept_loop_finished_
        and static_cast<uint64_t>(connection_slots_.available_permits())
              == max_connections_) {
      lifecycle_ = Lifecycle::done;
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
      auto pipeline = as<OpenPipeline<chunk_ptr>>(*sub);
      co_await pipeline.close();
    }
    co_return;
  }

  auto release_connection_slot() -> void {
    connection_slots_.add_permit();
  }

  auto finish_accept(Box<folly::coro::Transport> transport,
                     folly::SocketAddress peer, diagnostic_handler& dh)
    -> Task<void> {
    auto release_connection_slot_guard = detail::scope_guard{[this]() noexcept {
      release_connection_slot();
    }};
    if (tls_context_) {
      try {
        transport = Box<folly::coro::Transport>{
          co_await upgrade_transport_to_tls_server(std::move(*transport),
                                                   tls_context_)};
      } catch (folly::AsyncSocketException const& ex) {
        // Peer-driven TLS failures are expected at runtime; keep serving other
        // connections instead of failing the whole operator.
        diagnostic::warning("TLS handshake failed")
          .primary(args_.endpoint.source)
          .note("TLS handshake with peer {} failed", peer.describe())
          .note("reason: {}", ex.what())
          .hint("verify TLS settings and certificates on both sides")
          .emit(dh);
        co_return;
      }
    }
    TENZIR_DEBUG("accepted connection from {}", peer.describe());
    co_await message_queue_->enqueue(Accepted{std::move(transport)});
    release_connection_slot_guard.disable();
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_ASSERT(evb_);
    TENZIR_DEBUG("accept_tcp: accept loop started on {}", address_.describe());
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
        [this, &ctx]() -> Task<std::unique_ptr<folly::coro::Transport>> {
          try {
            // `await_task` is now queue-based, so accepting runs in this task.
            co_return co_await folly::coro::co_withExecutor(evb_,
                                                            server_->accept());
          } catch (folly::AsyncSocketException const& ex) {
            // Accept failures are per-connection network errors; keep the
            // listener alive and continue accepting new clients.
            diagnostic::warning("failed to accept incoming connection")
              .primary(args_.endpoint.source)
              .note("endpoint: {}", address_.describe())
              .note("reason: {}", ex.what())
              .emit(ctx.dh());
            throw;
          }
        },
        should_retry_accept);
      auto boxed
        = Box<folly::coro::Transport>::from_non_null(std::move(transport));
      auto peer = boxed->getPeerAddress();
      ctx.spawn_task(
        finish_accept(std::move(boxed), std::move(peer), ctx.dh()));
      release_connection_slot_guard.disable();
    }
  }

  static auto
  read_loop(uint64_t conn_id, Connection connection,
            OpenPipeline<chunk_ptr> pipeline, Arc<MessageQueue> message_queue,
            MetricsCounter bytes_counter) -> Task<void> {
    auto read_error = Option<std::string>{};
    auto cancellation_token
      = co_await folly::coro::co_current_cancellation_token;
    while (true) {
      try {
        auto read_result = co_await folly::coro::co_withCancellation(
          cancellation_token, read_tcp_chunk(*connection, buffer_size,
                                             std::chrono::milliseconds{0}));
        if (not read_result) {
          break;
        }
        bytes_counter.add((*read_result)->size());
        auto push_result = co_await pipeline.push(std::move(*read_result));
        TENZIR_UNUSED(push_result);
      } catch (folly::AsyncSocketException const& e) {
        read_error = e.what();
        break;
      }
    }
    co_await message_queue->enqueue(
      ConnectionClosed{conn_id, std::move(read_error)});
  }

  AcceptTcpArgs args_;
  folly::SocketAddress address_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  uint64_t max_connections_ = 128;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Semaphore connection_slots_;
  std::unordered_map<uint64_t, Connection> connections_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  bool accept_loop_finished_ = true;
  Lifecycle lifecycle_ = Lifecycle::running;
  mutable uint64_t next_conn_id_{0};
};

class AcceptTcpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.accept_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptTcpArgs, AcceptTcpListener>{};
    auto endpoint_arg = d.positional("endpoint", &AcceptTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &AcceptTcpArgs::tls);
    auto max_connections_arg
      = d.named("max_connections", &AcceptTcpArgs::max_connections);
    auto pipeline_arg = d.pipeline(&AcceptTcpArgs::user_pipeline,
                                   {{"peer", &AcceptTcpArgs::peer_info}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto ep_str, ctx.get(endpoint_arg));
      auto ep = to<struct endpoint>(ep_str.inner);
      auto loc = ctx.get_location(endpoint_arg).value_or(location::unknown);
      if (not ep) {
        diagnostic::error("failed to parse endpoint").primary(loc).emit(ctx);
      } else if (not ep->port) {
        diagnostic::error("port number is required").primary(loc).emit(ctx);
      }
      if (auto tls_val = ctx.get(tls_arg)) {
        auto tls_opts = tls_options{*tls_val, {.is_server = true}};
        if (auto valid = tls_opts.validate(ctx); not valid) {
          return {};
        }
      }
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

} // namespace tenzir::plugins::accept_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_tcp::AcceptTcpPlugin)
