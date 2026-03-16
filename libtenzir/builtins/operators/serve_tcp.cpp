//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
#include <tenzir/plugin.hpp>
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
  std::optional<located<data>> tls;
  std::optional<located<uint64_t>> max_connections;
  located<ir::pipeline> printer;
};

class ServeTcp final : public Operator<table_slice, void> {
public:
  struct Accepted {
    Box<folly::coro::Transport> client;
  };

  struct Payload {
    chunk_ptr chunk;
  };

  struct DrainCheck {};

  using Message = variant<Accepted, Payload, DrainCheck>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit ServeTcp(ServeTcpArgs args) : args_{std::move(args)} {
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
    if (args_.max_connections) {
      max_connections_ = args_.max_connections->inner;
    }
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (tls_ and tls_->get_tls(nullptr).inner) {
      auto context = tls_->make_folly_ssl_context(ctx);
      if (not context) {
        request_stop();
        co_return;
      }
      tls_context_ = std::move(*context);
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, listen_backlog);
    ctx.spawn_task(folly::coro::co_withCancellation(accept_cancel_->getToken(),
                                                    accept_loop(ctx.dh())));
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      request_stop();
      co_return;
    }
    auto sub = co_await ctx.spawn_sub(sub_key_, std::move(pipeline),
                                      tag_v<table_slice>);
    TENZIR_UNUSED(sub);
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
          co_return;
        }
        TENZIR_DEBUG("accepted client {}",
                     accepted.client->getPeerAddress().describe());
        clients_.push_back(std::move(accepted.client));
        co_return;
      },
      [&](Payload payload) -> Task<void> {
        if (lifecycle_ != Lifecycle::running or not payload.chunk
            or payload.chunk->size() == 0 or clients_.empty()) {
          co_return;
        }
        co_await broadcast_payload(payload.chunk, ctx.dh());
      },
      [&](DrainCheck) -> Task<void> {
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
      request_stop();
      co_return;
    }
    auto pipeline = as<OpenPipeline<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      request_stop();
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
        auto pipeline = as<OpenPipeline<table_slice>>(*sub);
        co_await pipeline.close();
      } else {
        request_stop();
      }
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    request_stop();
    co_return;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  static auto close_client(Box<folly::coro::Transport> client) -> void {
    auto* evb = client->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([client = std::move(client)]() mutable {
      client->close();
    });
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

  auto request_stop() -> void {
    if (lifecycle_ == Lifecycle::done) {
      return;
    }
    begin_draining();
    close_all_clients();
    maybe_finish_draining();
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ == Lifecycle::draining
        and reserved_connections_.load(std::memory_order_relaxed) == 0) {
      lifecycle_ = Lifecycle::done;
    }
  }

  auto close_all_clients() -> void {
    for (auto& client : clients_) {
      close_client(std::move(client));
      release_connection_slot();
    }
    clients_.clear();
  }

  auto write_to_client(folly::coro::Transport& client, folly::ByteRange data)
    -> Task<bool> {
    try {
      co_await folly::coro::co_withExecutor(evb_, client.write(data));
      co_return true;
    } catch (folly::AsyncSocketException const&) {
      // TODO: Surface peer disconnects and other routine TCP write failures
      // as metrics instead of warnings in a follow-up that covers all TCP
      // operators.
      co_return false;
    }
  }

  auto release_connection_slot_and_notify() -> Task<void> {
    release_connection_slot();
    co_await message_queue_->enqueue(DrainCheck{});
  }

  auto broadcast_payload(chunk_ptr const& chunk, diagnostic_handler& dh)
    -> Task<void> {
    TENZIR_UNUSED(dh);
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    for (size_t i = 0; i < clients_.size();) {
      auto ok = co_await write_to_client(*clients_[i], data);
      if (ok) {
        ++i;
        continue;
      }
      close_client(std::move(clients_[i]));
      release_connection_slot();
      clients_.erase(clients_.begin() + i);
    }
  }

  auto accept_loop(diagnostic_handler& dh) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_DEBUG("serve_tcp: accept loop started on {}", address_.describe());
    auto cancellation_token = accept_cancel_->getToken();
    while (not cancellation_token.isCancellationRequested()) {
      auto accept_error = Option<std::string>{};
      try {
        auto transport
          = co_await folly::coro::co_withExecutor(evb_, server_->accept());
        auto client
          = Box<folly::coro::Transport>::from_non_null(std::move(transport));
        if (cancellation_token.isCancellationRequested()) {
          close_client(std::move(client));
          co_return;
        }
        auto peer = client->getPeerAddress().describe();
        if (not try_acquire_connection_slot()) {
          diagnostic::warning(
            "connection rejected: maximum number of connections reached")
            .primary(args_.endpoint.source)
            .note("peer: {}", peer)
            .note("max_connections: {}", max_connections_)
            .hint("increase `max_connections` if this level of concurrency is "
                  "expected")
            .emit(dh);
          close_client(std::move(client));
          continue;
        }
        auto release_connection_slot_guard = folly::makeGuard([&] {
          release_connection_slot();
        });
        if (tls_context_) {
          auto tls_error = Option<std::string>{};
          auto tls_cancelled = false;
          try {
            // TODO: Move TLS handshakes into an async preparation stage that
            // posts ready clients back into the queue without introducing
            // shared mutable state. The current conservative approach keeps
            // fan-out non-blocking, but serializes the accept loop on
            // handshake work.
            client = Box<folly::coro::Transport>{
              co_await upgrade_transport_to_tls_server(std::move(*client),
                                                       tls_context_)};
          } catch (folly::AsyncSocketException const& ex) {
            if (cancellation_token.isCancellationRequested()
                or lifecycle_ != Lifecycle::running) {
              tls_cancelled = true;
            } else {
              tls_error = ex.what();
            }
          }
          if (tls_cancelled) {
            co_await release_connection_slot_and_notify();
            release_connection_slot_guard.dismiss();
            co_return;
          }
          if (tls_error) {
            diagnostic::warning("TLS handshake failed")
              .primary(args_.endpoint.source)
              .note("peer: {}", peer)
              .note("reason: {}", *tls_error)
              .hint("verify TLS settings and certificates on both sides")
              .emit(dh);
            co_await release_connection_slot_and_notify();
            release_connection_slot_guard.dismiss();
            continue;
          }
        }
        if (cancellation_token.isCancellationRequested()
            or lifecycle_ != Lifecycle::running) {
          close_client(std::move(client));
          co_await release_connection_slot_and_notify();
          release_connection_slot_guard.dismiss();
          co_return;
        }
        TENZIR_DEBUG("serve_tcp: accepted {}", peer);
        co_await message_queue_->enqueue(Accepted{std::move(client)});
        release_connection_slot_guard.dismiss();
        continue;
      } catch (folly::AsyncSocketException const& ex) {
        // Accept failures are per-connection network errors; keep the
        // listener alive and continue accepting new clients.
        if (cancellation_token.isCancellationRequested()) {
          co_return;
        }
        accept_error = ex.what();
      }
      if (accept_error) {
        diagnostic::warning("failed to accept incoming connection")
          .primary(args_.endpoint.source)
          .note("endpoint: {}", address_.describe())
          .note("reason: {}", *accept_error)
          .emit(dh);
        co_await folly::coro::sleep(accept_retry_delay);
      }
    }
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

  ServeTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  mutable Box<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  Atomic<uint64_t> reserved_connections_{0};
  std::vector<Box<folly::coro::Transport>> clients_;
  uint64_t max_connections_ = 128;
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
    auto printer_arg = d.pipeline(&ServeTcpArgs::printer);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_str, ctx.get(endpoint_arg));
      auto ep = to<struct endpoint>(endpoint_str.inner);
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::serve_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::serve_tcp::ServeTcpPlugin)
