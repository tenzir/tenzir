//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/metrics.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/async/tls.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <limits>
#include <memory>

namespace tenzir::plugins::serve_tcp {

namespace {

// Match a typical TCP server listen queue depth: large enough for short bursts
// of incoming connections without implying that we expect unbounded fan-in.
constexpr auto listen_backlog = uint32_t{128};
// Retry accepts quickly after transient socket errors so the listener recovers
// fast, while still backing off enough to avoid a tight warning loop.
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
// The listener queue only carries accepted sockets, so a modest fixed
// capacity is sufficient while still tolerating short bursts.
constexpr auto message_queue_capacity = uint32_t{512};

struct ServeTcpArgs {
  located<std::string> endpoint;
  Option<located<data>> tls;
  Option<located<uint64_t>> max_connections;
  located<ir::pipeline> printer;
};

struct TcpConnectionMetrics {
  TcpConnectionMetrics(folly::coro::Transport const& transport,
                       metric_handler handler)
    : handle{transport.getPeerAddress().describe()},
      handler{std::move(handler)} {
    TENZIR_ASSERT(transport.getPeerAddress().isFamilyInet());
  }

  auto record_write(size_t bytes) -> void {
    writes.fetch_add(1, std::memory_order_relaxed);
    bytes_written.fetch_add(bytes, std::memory_order_relaxed);
  }

  auto emit() -> void {
    handler.emit({
      {"handle", handle},
      {"reads", uint64_t{0}},
      {"writes", writes.exchange(0, std::memory_order_relaxed)},
      {"bytes_read", uint64_t{0}},
      {"bytes_written", bytes_written.exchange(0, std::memory_order_relaxed)},
    });
  }

  auto close() -> void {
    closed.store(true, std::memory_order_relaxed);
    emit();
  }

  auto is_closed() const -> bool {
    return closed.load(std::memory_order_relaxed);
  }

  std::string handle;
  metric_handler handler;
  Atomic<uint64_t> writes = {};
  Atomic<uint64_t> bytes_written = {};
  Atomic<bool> closed = false;
};

auto tcp_metrics_type() -> type {
  return {
    "tenzir.metrics.tcp",
    record_type{
      {"handle", string_type{}},
      {"reads", uint64_type{}},
      {"writes", uint64_type{}},
      {"bytes_read", uint64_type{}},
      {"bytes_written", uint64_type{}},
    },
  };
}

auto emit_tcp_metrics(Arc<TcpConnectionMetrics> metrics) -> Task<void> {
  while (true) {
    co_await sleep_for(defaults::metrics_interval);
    if (metrics->is_closed()) {
      co_return;
    }
    metrics->emit();
  }
}

class ServeTcp final : public Operator<table_slice, void> {
public:
  struct Accepted {
    Box<folly::coro::Transport> client;
  };

  struct Payload {
    chunk_ptr chunk;
  };

  struct AcceptLoopFinished {};

  using Message = variant<Accepted, Payload, AcceptLoopFinished>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  struct Client {
    Box<folly::coro::Transport> transport;
    Arc<TcpConnectionMetrics> metrics;
  };

  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit ServeTcp(ServeTcpArgs args)
    : args_{std::move(args)},
      max_connections_{args_.max_connections ? args_.max_connections->inner
                                             : uint64_t{128}},
      connection_slots_{detail::narrow<size_t>(max_connections_)} {
    auto ep = to<Endpoint>(args_.endpoint.inner);
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

  auto start(OpCtx& ctx) -> Task<void> override {
    if (tls_) {
      auto resolved = tls_->resolve(ctx.actor_system().config(), ctx);
      if (not resolved) {
        co_await request_stop();
        co_return;
      }
      if (resolved->tls.inner) {
        auto context = resolved->make_folly_ssl_context(ctx);
        if (not context) {
          co_await request_stop();
          co_return;
        }
        tls_context_ = std::move(*context);
      }
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, listen_backlog);
    accept_loop_finished_ = false;
    tcp_metrics_ = make_metric_handler(ctx, tcp_metrics_type());
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
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      co_await request_stop();
      co_return;
    }
    co_await ctx.spawn_sub<table_slice>(sub_key_, std::move(pipeline));
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* message_ptr = result.try_as<Message>();
    if (not message_ptr) {
      co_return;
    }
    auto message = std::move(*message_ptr);
    co_await co_match(
      std::move(message),
      [&](Accepted accepted) -> Task<void> {
        if (lifecycle_ != Lifecycle::running) {
          close_client(std::move(accepted.client));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        TENZIR_DEBUG("accepted client {}",
                     accepted.client->getPeerAddress().describe());
        auto metrics = Arc<TcpConnectionMetrics>{
          std::in_place,
          *accepted.client,
          tcp_metrics_,
        };
        ctx.spawn_task(emit_tcp_metrics(metrics));
        clients_.push_back({
          .transport = std::move(accepted.client),
          .metrics = std::move(metrics),
        });
        co_return;
      },
      [&](Payload payload) -> Task<void> {
        if (lifecycle_ != Lifecycle::running or not payload.chunk
            or payload.chunk->size() == 0 or clients_.empty()) {
          co_return;
        }
        co_await broadcast_payload(payload.chunk, ctx.dh());
      },
      [&](AcceptLoopFinished) -> Task<void> {
        accept_loop_finished_ = true;
        maybe_finish_draining();
        co_return;
      });
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (not sub) {
      co_await request_stop();
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      co_await request_stop();
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running or not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await message_queue_->enqueue(Payload{std::move(chunk)});
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      begin_draining();
      if (auto sub = ctx.get_sub(make_view(sub_key_))) {
        auto& pipeline = as<SubHandle<table_slice>>(*sub);
        co_await pipeline.close();
      } else {
        co_await request_stop();
      }
    }
    maybe_finish_draining();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    co_await request_stop();
    co_return;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  static auto close_client(Box<folly::coro::Transport> client) -> void {
    auto* evb = client->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([client = std::move(client)]() mutable {
      client->close();
    });
  }

  static auto close_client(Client client) -> void {
    client.metrics->close();
    close_client(std::move(client.transport));
  }

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

  auto begin_draining() -> void {
    if (lifecycle_ != Lifecycle::running) {
      return;
    }
    lifecycle_ = Lifecycle::draining;
    stop_accepting();
  }

  auto request_stop() -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    begin_draining();
    close_all_clients();
    maybe_finish_draining();
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ != Lifecycle::draining) {
      return;
    }
    // All connection permits must be returned *and* the accept loop must have
    // finished before we can safely transition to done.
    if (accept_loop_finished_
        and static_cast<uint64_t>(connection_slots_.available_permits())
              == max_connections_) {
      lifecycle_ = Lifecycle::done;
    }
  }

  auto release_connection_slot() -> void {
    connection_slots_.add_permit();
  }

  auto close_all_clients() -> void {
    for (auto& client : clients_) {
      close_client(std::move(client));
      release_connection_slot();
    }
    clients_.clear();
  }

  auto write_to_client(Client& client, folly::ByteRange data) -> Task<bool> {
    auto* client_evb = client.transport->getEventBase();
    TENZIR_ASSERT(client_evb);
    try {
      co_await folly::coro::co_withExecutor(client_evb,
                                            client.transport->write(data));
      client.metrics->record_write(data.size());
      co_return true;
    } catch (folly::AsyncSocketException const&) {
      // TODO: Surface peer disconnects and other routine TCP write failures
      // as metrics instead of warnings in a follow-up that covers all TCP
      // operators.
      co_return false;
    }
  }

  auto broadcast_payload(chunk_ptr const& chunk, diagnostic_handler& dh)
    -> Task<void> {
    TENZIR_UNUSED(dh);
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    for (size_t i = 0; i < clients_.size();) {
      auto ok = co_await write_to_client(clients_[i], data);
      if (ok) {
        ++i;
        continue;
      }
      close_client(std::move(clients_[i]));
      release_connection_slot();
      clients_.erase(clients_.begin() + i);
      maybe_finish_draining();
    }
  }

  auto finish_accept(Box<folly::coro::Transport> client, std::string peer,
                     diagnostic_handler& dh) -> Task<void> {
    auto release_connection_slot_guard = detail::scope_guard{[this]() noexcept {
      release_connection_slot();
    }};
    if (tls_context_) {
      try {
        client = Box<folly::coro::Transport>{
          co_await upgrade_transport_to_tls_server(std::move(*client),
                                                   tls_context_)};
      } catch (folly::AsyncSocketException const& ex) {
        diagnostic::warning("TLS handshake failed")
          .primary(args_.endpoint.source)
          .note("peer: {}", peer)
          .note("reason: {}", ex.what())
          .hint("verify TLS settings and certificates on both sides")
          .emit(dh);
        co_return;
      }
    }
    TENZIR_DEBUG("serve_tcp: accepted {}", peer);
    co_await message_queue_->enqueue(Accepted{std::move(client)});
    release_connection_slot_guard.disable();
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_DEBUG("serve_tcp: accept loop started on {}", address_.describe());
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
      auto client
        = Box<folly::coro::Transport>::from_non_null(std::move(transport));
      auto peer = client->getPeerAddress().describe();
      ctx.spawn_task(
        finish_accept(std::move(client), std::move(peer), ctx.dh()));
      release_connection_slot_guard.disable();
    }
  }

  ServeTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  uint64_t max_connections_ = 128;
  mutable Box<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Semaphore connection_slots_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  std::vector<Client> clients_;
  metric_handler tcp_metrics_ = {};
  bool accept_loop_finished_ = true;
  Lifecycle lifecycle_ = Lifecycle::running;
};

class ServeTcpPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.serve_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<ServeTcpArgs, ServeTcp>{};
    auto endpoint_arg = d.positional("endpoint", &ServeTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &ServeTcpArgs::tls);
    auto max_connections_arg
      = d.named("max_connections", &ServeTcpArgs::max_connections);
    auto printer_arg
      = d.pipeline(&ServeTcpArgs::printer, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_str, ctx.get(endpoint_arg));
      auto ep = to<Endpoint>(endpoint_str.inner);
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
      TRY(auto printer, ctx.get(printer_arg));
      auto output = printer.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(printer.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.invariant_order_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::serve_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::serve_tcp::ServeTcpPlugin)
