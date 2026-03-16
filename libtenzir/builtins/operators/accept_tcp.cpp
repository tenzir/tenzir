//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/tcp.hpp>
#include <tenzir/async/tls.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
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
#include <folly/ScopeGuard.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Sleep.h>
#include <folly/executors/GlobalExecutor.h>
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

  using Message = variant<Accepted, ConnectionClosed>;

  using MessageQueue = folly::coro::BoundedQueue<Message>;

  AcceptTcpListener(AcceptTcpArgs args)
    : endpoint_source_{args.endpoint.source},
      user_pipeline_{std::move(args.user_pipeline)},
      peer_let_id_{args.peer_info} {
    // Parse endpoint string to SocketAddress (validation already done in
    // describe)
    auto ep = to<struct endpoint>(args.endpoint.inner);
    TENZIR_ASSERT(ep);
    TENZIR_ASSERT(ep->port);
    if (ep->host.empty()) {
      address_.setFromLocalPort(ep->port->number());
    } else {
      address_.setFromHostPort(ep->host, ep->port->number());
    }
    if (args.tls) {
      tls_ = tls_options{*args.tls, {.is_server = true}};
    }
    if (args.max_connections) {
      max_connections_ = args.max_connections->inner;
    }
  }

  AcceptTcpListener(const AcceptTcpListener&) = delete;
  AcceptTcpListener& operator=(const AcceptTcpListener&) = delete;
  AcceptTcpListener(AcceptTcpListener&&) noexcept = default;
  AcceptTcpListener& operator=(AcceptTcpListener&&) noexcept = default;
  ~AcceptTcpListener() override = default;

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
    accept_loop_done_ = false;
    ctx.spawn_task([this, &ctx]() -> Task<void> {
      auto notify_done = folly::makeGuard([this] {
        accept_loop_done_ = true;
        accept_loop_notify_->notify_one();
      });
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
        auto keep_connection_slot = false;
        auto release_connection_slot_guard = folly::makeGuard([&] {
          if (not keep_connection_slot) {
            release_connection_slot();
          }
        });
        if (lifecycle_ != Lifecycle::running) {
          close_transport(std::move(transport));
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
        auto pipeline_copy = user_pipeline_.inner;
        auto env = substitute_ctx::env_t{};
        env[peer_let_id_] = std::move(peer_record);
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
          co_return;
        }
        auto key = sub_key_for(conn_id);
        auto sub = co_await ctx.spawn_sub(
          std::move(key), std::move(pipeline_copy), tag_v<chunk_ptr>);
        auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
        auto connection = Connection{std::move(*transport)};
        auto [_, inserted] = connections_.emplace(conn_id, connection);
        TENZIR_ASSERT(inserted);
        keep_connection_slot = true;
        auto message_queue = message_queue_;
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb,
          read_loop(conn_id, std::move(connection), std::move(open_pipeline),
                    std::move(message_queue), std::move(bytes_read))));
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (closed.error) {
          // TODO: Surface routine TCP read failures and disconnects as metrics
          // in a follow-up that covers all TCP operators.
          diagnostic::warning("connection closed after read error")
            .primary(endpoint_source_)
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
      });
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto conn_id = static_cast<uint64_t>(as<int64_t>(key));
    if (auto it = connections_.find(conn_id); it != connections_.end()) {
      auto connection = it->second;
      connections_.erase(it);
      release_connection_slot();
      maybe_finish_draining();
      close_transport(std::move(connection));
    }
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    lifecycle_ = Lifecycle::draining;
    stop_accepting();
    close_all_connections();
    co_await wait_for_accept_loop();
    co_return maybe_finish_draining() ? FinalizeBehavior::done
                                      : FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
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
    }
  }

  auto maybe_finish_draining() -> bool {
    if (lifecycle_ == Lifecycle::draining and outstanding_connections() == 0) {
      lifecycle_ = Lifecycle::done;
    }
    return lifecycle_ == Lifecycle::done;
  }

  auto outstanding_connections() const -> uint64_t {
    return reserved_connections_.load(std::memory_order_relaxed);
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

  auto wait_for_accept_loop() -> Task<void> {
    if (accept_loop_done_) {
      co_return;
    }
    co_await accept_loop_notify_->wait();
  }

  auto finish_accept(Box<folly::coro::Transport> transport,
                     folly::SocketAddress peer, diagnostic_handler& dh)
    -> Task<void> {
    auto keep_connection_slot = false;
    auto release_connection_slot_guard = folly::makeGuard([&] {
      if (not keep_connection_slot) {
        release_connection_slot();
      }
    });
    if (tls_context_) {
      try {
        transport = Box<folly::coro::Transport>{
          co_await upgrade_transport_to_tls_server(std::move(*transport),
                                                   tls_context_)};
      } catch (folly::AsyncSocketException const& ex) {
        if (accept_cancel_->getToken().isCancellationRequested()) {
          co_return;
        }
        // Peer-driven TLS failures are expected at runtime; keep serving other
        // connections instead of failing the whole operator.
        diagnostic::warning("TLS handshake failed")
          .primary(endpoint_source_)
          .note("TLS handshake with peer {} failed", peer.describe())
          .note("reason: {}", ex.what())
          .hint("verify TLS settings and certificates on both sides")
          .emit(dh);
        co_return;
      }
    }
    if (accept_cancel_->getToken().isCancellationRequested()) {
      close_transport(std::move(transport));
      co_return;
    }
    TENZIR_DEBUG("accepted connection from {}", peer.describe());
    co_await message_queue_->enqueue(Accepted{std::move(transport)});
    keep_connection_slot = true;
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_ASSERT(evb_);
    auto cancellation_token = accept_cancel_->getToken();
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      while (not cancellation_token.isCancellationRequested()) {
        std::unique_ptr<folly::coro::Transport> transport;
        auto accept_error = Option<std::string>{};
        try {
          co_await folly::coro::co_safe_point;
          // `await_task` is now queue-based, so accepting runs in this task.
          transport
            = co_await folly::coro::co_withExecutor(evb_, server_->accept());
        } catch (folly::AsyncSocketException const& ex) {
          // Accept can fail transiently (e.g., client abort/reset). Keep the
          // listener alive and continue with the next accept attempt.
          if (cancellation_token.isCancellationRequested()) {
            co_return;
          }
          accept_error = ex.what();
        }
        if (accept_error) {
          diagnostic::warning("failed to accept incoming connection")
            .primary(endpoint_source_)
            .note("endpoint: {}", address_.describe())
            .note("reason: {}", *accept_error)
            .emit(ctx.dh());
          co_await folly::coro::sleep(accept_retry_delay);
          continue;
        }
        auto boxed
          = Box<folly::coro::Transport>::from_non_null(std::move(transport));
        if (cancellation_token.isCancellationRequested()) {
          close_transport(std::move(boxed));
          co_return;
        }
        auto peer = boxed->getPeerAddress();
        if (not try_acquire_connection_slot()) {
          diagnostic::warning(
            "connection rejected: maximum number of connections reached")
            .primary(endpoint_source_)
            .note("peer: {}", peer.describe())
            .note("max_connections: {}", max_connections_)
            .hint("increase `max_connections` if this level of concurrency is "
                  "expected")
            .emit(ctx.dh());
          close_transport(std::move(boxed));
          continue;
        }
        scope.spawn(finish_accept(std::move(boxed), std::move(peer), ctx.dh()));
      }
    });
  }

  auto try_acquire_connection_slot() -> bool {
    auto current = reserved_connections_.load(std::memory_order_relaxed);
    while (current < max_connections_) {
      if (reserved_connections_.compare_exchange_weak(
            current, current + 1, std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  auto release_connection_slot() -> void {
    auto previous
      = reserved_connections_.fetch_sub(1, std::memory_order_relaxed);
    TENZIR_ASSERT(previous > 0);
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

  location endpoint_source_ = location::unknown;
  folly::SocketAddress address_;
  located<ir::pipeline> user_pipeline_;
  let_id peer_let_id_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  std::unordered_map<uint64_t, Connection> connections_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  Box<Notify> accept_loop_notify_{std::in_place};
  bool accept_loop_done_ = true;
  Atomic<uint64_t> reserved_connections_{0};
  uint64_t max_connections_ = 128;
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
          max_connections and max_connections->inner == 0) {
        auto loc
          = ctx.get_location(max_connections_arg).value_or(location::unknown);
        diagnostic::error("max_connections must be greater than 0")
          .primary(loc)
          .emit(ctx);
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
