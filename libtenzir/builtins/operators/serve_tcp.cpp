//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/mutex.hpp>
#include <tenzir/async/tls.hpp>
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
#include <folly/OperationCancelled.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <algorithm>
#include <atomic>
#include <memory>

namespace tenzir::plugins::serve_tcp {

namespace {

constexpr auto listen_backlog = uint32_t{128};
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
constexpr auto write_timeout = std::chrono::seconds{5};
constexpr auto message_queue_capacity = uint32_t{512};

struct ServeTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  std::optional<located<uint64_t>> max_connections;
  located<ir::pipeline> printer;
};

class ServeTcp final : public Operator<table_slice, void> {
public:
  struct ClientHandle {
    explicit ClientHandle(Box<folly::coro::Transport> transport)
      : transport{std::move(transport)} {
    }

    Box<folly::coro::Transport> transport;
  };

  using Client = Arc<ClientHandle>;

  struct Accepted {
    Box<folly::coro::Transport> client;
  };

  struct Wakeup {};

  using Message = variant<Accepted, Wakeup>;
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

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_->load() != Lifecycle::running) {
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

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> override {
    auto clients = std::vector<Client>{};
    {
      auto guard = co_await clients_mutex_->lock();
      if (lifecycle_->load() == Lifecycle::done or not chunk
          or chunk->size() == 0 or clients_.empty()) {
        co_return;
      }
      clients = clients_;
    }
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    auto diagnostics = std::vector<std::optional<diagnostic>>{};
    diagnostics.resize(clients.size());
    auto write_tasks = std::vector<Task<void>>{};
    write_tasks.reserve(clients.size());
    for (size_t i = 0; i < clients.size(); ++i) {
      write_tasks.push_back(write_to_client(clients[i], data, diagnostics[i]));
    }
    co_await folly::coro::collectAllRange(std::move(write_tasks));
    TENZIR_ASSERT(diagnostics.size() == clients.size());
    auto guard = co_await clients_mutex_->lock();
    for (size_t i = 0; i < diagnostics.size(); ++i) {
      auto& maybe_diagnostic = diagnostics[i];
      if (not maybe_diagnostic) {
        continue;
      }
      ctx.dh().emit(std::move(*maybe_diagnostic));
      auto* failed_client = &*clients[i];
      auto new_end = std::remove_if(clients_.begin(), clients_.end(),
                                    [&](Client const& client) {
                                      return &*client == failed_client;
                                    });
      auto removed = new_end != clients_.end();
      clients_.erase(new_end, clients_.end());
      if (removed) {
        close_client(std::move(clients[i]));
      }
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (lifecycle_->load() == Lifecycle::done) {
      co_return {};
    }
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_->load() == Lifecycle::done) {
      co_return;
    }
    auto* message_ptr = result.try_as<Message>();
    if (not message_ptr) {
      co_return;
    }
    auto message = std::move(*message_ptr);
    if (auto* accepted = std::get_if<Accepted>(&message)) {
      if (lifecycle_->load() != Lifecycle::running) {
        close_client(std::move(accepted->client));
        co_return;
      }
      co_await add_client(std::move(accepted->client), ctx.dh());
    }
    co_return;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    auto lifecycle = lifecycle_->load();
    if (lifecycle == Lifecycle::done) {
      co_await close_all_clients();
      co_return FinalizeBehavior::done;
    }
    if (lifecycle == Lifecycle::running) {
      lifecycle_->store(Lifecycle::draining);
      stop_accepting();
      if (auto sub = ctx.get_sub(make_view(sub_key_))) {
        auto pipeline = as<OpenPipeline<table_slice>>(*sub);
        co_await pipeline.close();
      } else {
        request_stop();
      }
    }
    if (lifecycle_->load() == Lifecycle::done) {
      co_await close_all_clients();
      co_return FinalizeBehavior::done;
    }
    co_return FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    if (lifecycle_->load() == Lifecycle::done) {
      co_await close_all_clients();
      co_return;
    }
    request_stop();
    co_await close_all_clients();
    co_return;
  }

  auto state() -> OperatorState override {
    return lifecycle_->load() == Lifecycle::done ? OperatorState::done
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

  static auto close_client(Client client) -> void {
    auto* evb = client->transport->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([client = std::move(client)]() mutable {
      client->transport->close();
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

  auto request_stop() -> void {
    if (lifecycle_->exchange(Lifecycle::done) == Lifecycle::done) {
      return;
    }
    stop_accepting();
    TENZIR_UNUSED(message_queue_->try_enqueue(Wakeup{}));
  }

  auto close_all_clients() -> Task<void> {
    auto guard = co_await clients_mutex_->lock();
    auto clients = std::move(clients_);
    for (auto& client : clients) {
      close_client(std::move(client));
    }
    co_return;
  }

  auto write_to_client(Client client, folly::ByteRange data,
                       std::optional<diagnostic>& maybe_diagnostic)
    -> Task<void> {
    try {
      co_await folly::coro::timeout(
        folly::coro::co_withExecutor(evb_, client->transport->write(data)),
        write_timeout);
      maybe_diagnostic.reset();
      co_return;
    } catch (folly::FutureTimeout const& ex) {
      auto peer = client->transport->getPeerAddress().describe();
      maybe_diagnostic
        = diagnostic::warning("failed to write to client {}", peer)
            .primary(args_.endpoint.source)
            .note("reason: {}", ex.what())
            .note("dropping this client and continuing with "
                  "remaining connections")
            .done();
      co_return;
    } catch (folly::AsyncSocketException const& ex) {
      auto peer = client->transport->getPeerAddress().describe();
      maybe_diagnostic
        = diagnostic::warning("failed to write to client {}", peer)
            .primary(args_.endpoint.source)
            .note("reason: {}", ex.what())
            .note("dropping this client and continuing with "
                  "remaining connections")
            .done();
      co_return;
    }
  }

  auto add_client(Box<folly::coro::Transport> client, diagnostic_handler& dh)
    -> Task<void> {
    auto peer = client->getPeerAddress().describe();
    auto reject_max_connections = [&]() {
      diagnostic::warning(
        "connection rejected: maximum number of connections reached")
        .primary(args_.endpoint.source)
        .note("peer: {}", peer)
        .note("max_connections: {}", max_connections_)
        .hint("increase `max_connections` if this level of concurrency is "
              "expected")
        .emit(dh);
      close_client(std::move(client));
    };
    {
      auto guard = co_await clients_mutex_->lock();
      if (lifecycle_->load() != Lifecycle::running) {
        close_client(std::move(client));
        co_return;
      }
      if (clients_.size() >= max_connections_) {
        reject_max_connections();
        co_return;
      }
    }
    if (tls_context_) {
      try {
        co_await upgrade_transport_to_tls_server(client, tls_context_);
      } catch (folly::AsyncSocketException const& ex) {
        auto peer = client->getPeerAddress().describe();
        diagnostic::warning("TLS handshake failed")
          .primary(args_.endpoint.source)
          .note("peer: {}", peer)
          .note("reason: {}", ex.what())
          .hint("verify TLS settings and certificates on both sides")
          .emit(dh);
        close_client(std::move(client));
        co_return;
      }
    }
    auto guard = co_await clients_mutex_->lock();
    if (lifecycle_->load() != Lifecycle::running) {
      close_client(std::move(client));
      co_return;
    }
    if (clients_.size() >= max_connections_) {
      reject_max_connections();
      co_return;
    }
    TENZIR_DEBUG("accepted client {}", client->getPeerAddress().describe());
    clients_.push_back(Arc<ClientHandle>{ClientHandle{std::move(client)}});
    co_return;
  }

  auto accept_loop(diagnostic_handler& dh) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_DEBUG("serve_tcp: accept loop started on {}", address_.describe());
    try {
      while (lifecycle_->load() == Lifecycle::running) {
        auto accept_error = Option<std::string>{};
        try {
          auto transport
            = co_await folly::coro::co_withExecutor(evb_, server_->accept());
          auto client
            = Box<folly::coro::Transport>::from_non_null(std::move(transport));
          if (lifecycle_->load() != Lifecycle::running) {
            close_client(std::move(client));
            co_return;
          }
          TENZIR_DEBUG("serve_tcp: accepted {}",
                       client->getPeerAddress().describe());
          co_await message_queue_->enqueue(Accepted{std::move(client)});
          continue;
        } catch (folly::AsyncSocketException const& ex) {
          // Accept failures are per-connection network errors; keep the
          // listener alive and continue accepting new clients.
          if (lifecycle_->load() != Lifecycle::running) {
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
        if (lifecycle_->load() != Lifecycle::running) {
          co_return;
        }
      }
    } catch (folly::OperationCancelled const&) {
      co_return;
    }
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
  Box<RawMutex> clients_mutex_{std::in_place};
  std::vector<Client> clients_;
  uint64_t max_connections_ = 128;
  Box<std::atomic<Lifecycle>> lifecycle_{std::in_place, Lifecycle::running};
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
