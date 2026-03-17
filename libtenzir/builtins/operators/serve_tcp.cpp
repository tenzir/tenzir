//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/tls.hpp>
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
  std::optional<located<data>> tls;
  std::optional<located<uint64_t>> max_connections;
  located<ir::pipeline> printer;
};

class ServeTcp final : public Operator<table_slice, void> {
public:
  struct ConnectionSlot {};

  struct Accepted {
    Box<folly::coro::Transport> client;
    ConnectionSlot slot;
  };

  struct Payload {
    chunk_ptr chunk;
  };

  using Message = variant<Accepted, Payload>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;
  using SlotQueue = folly::coro::BoundedQueue<ConnectionSlot>;

  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit ServeTcp(ServeTcpArgs args)
    : args_{std::move(args)},
      max_connections_{args_.max_connections ? args_.max_connections->inner
                                             : uint64_t{128}},
      slot_queue_{make_slot_queue(max_connections_)} {
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
    for (auto i = uint64_t{0}; i < max_connections_; ++i) {
      auto success = slot_queue_->try_enqueue(ConnectionSlot{});
      TENZIR_ASSERT(success);
    }
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (tls_ and tls_->get_tls(nullptr).inner) {
      auto context = tls_->make_folly_ssl_context(ctx);
      if (not context) {
        co_await request_stop();
        co_return;
      }
      tls_context_ = std::move(*context);
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, listen_backlog);
    accept_loop_ = ctx.spawn_task([this, &ctx]() -> Task<void> {
      try {
        co_await folly::coro::co_withCancellation(accept_cancel_->getToken(),
                                                  accept_loop(ctx.dh()));
      } catch (folly::OperationCancelled const&) {
        co_return;
      }
    });
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      co_await request_stop();
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
          release_connection_slot(std::move(accepted.slot));
          co_return;
        }
        TENZIR_DEBUG("accepted client {}",
                     accepted.client->getPeerAddress().describe());
        clients_.push_back(
          Client{std::move(accepted.client), std::move(accepted.slot)});
        co_return;
      },
      [&](Payload payload) -> Task<void> {
        if (lifecycle_ != Lifecycle::running or not payload.chunk
            or payload.chunk->size() == 0 or clients_.empty()) {
          co_return;
        }
        co_await broadcast_payload(payload.chunk, ctx.dh());
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
    auto pipeline = as<OpenPipeline<table_slice>>(*sub);
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
        auto pipeline = as<OpenPipeline<table_slice>>(*sub);
        co_await pipeline.close();
      } else {
        co_await request_stop();
      }
    }
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
                                         : OperatorState::unspecified;
  }

private:
  struct Client {
    Box<folly::coro::Transport> transport;
    ConnectionSlot slot;
  };

  static auto make_slot_queue(uint64_t max_connections) -> Box<SlotQueue> {
    TENZIR_ASSERT(max_connections <= std::numeric_limits<uint32_t>::max());
    return Box<SlotQueue>{std::in_place,
                          static_cast<uint32_t>(max_connections)};
  }

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

  auto join_accept_loop() -> Task<void> {
    if (not accept_loop_) {
      co_return;
    }
    auto accept_loop = std::move(*accept_loop_);
    accept_loop_ = None{};
    co_await accept_loop.join();
  }

  auto request_stop() -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    begin_draining();
    co_await join_accept_loop();
    close_all_clients();
    maybe_finish_draining();
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ == Lifecycle::draining and clients_.empty()) {
      lifecycle_ = Lifecycle::done;
    }
  }

  auto try_acquire_connection_slot() -> Option<ConnectionSlot> {
    auto slot = slot_queue_->try_dequeue();
    if (not slot) {
      return None{};
    }
    return std::move(*slot);
  }

  auto release_connection_slot(ConnectionSlot slot) -> void {
    auto success = slot_queue_->try_enqueue(std::move(slot));
    TENZIR_ASSERT(success);
  }

  auto close_all_clients() -> void {
    for (auto& client : clients_) {
      close_client(std::move(client.transport));
      release_connection_slot(std::move(client.slot));
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

  auto broadcast_payload(chunk_ptr const& chunk, diagnostic_handler& dh)
    -> Task<void> {
    TENZIR_UNUSED(dh);
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    for (size_t i = 0; i < clients_.size();) {
      auto ok = co_await write_to_client(*clients_[i].transport, data);
      if (ok) {
        ++i;
        continue;
      }
      close_client(std::move(clients_[i].transport));
      release_connection_slot(std::move(clients_[i].slot));
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
        auto slot = try_acquire_connection_slot();
        if (not slot) {
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
          release_connection_slot(std::move(*slot));
        });
        if (tls_context_) {
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
            if (cancellation_token.isCancellationRequested()) {
              close_client(std::move(client));
              co_return;
            }
            diagnostic::warning("TLS handshake failed")
              .primary(args_.endpoint.source)
              .note("peer: {}", peer)
              .note("reason: {}", ex.what())
              .hint("verify TLS settings and certificates on both sides")
              .emit(dh);
            continue;
          }
        }
        if (cancellation_token.isCancellationRequested()) {
          close_client(std::move(client));
          co_return;
        }
        TENZIR_DEBUG("serve_tcp: accepted {}", peer);
        co_await message_queue_->enqueue(
          Accepted{std::move(client), std::move(*slot)});
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
  Box<SlotQueue> slot_queue_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  Option<AsyncHandle<void>> accept_loop_;
  std::vector<Client> clients_;
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
