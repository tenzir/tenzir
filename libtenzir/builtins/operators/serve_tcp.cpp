//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <atomic>

namespace tenzir::plugins::serve_tcp {

namespace {

constexpr auto listen_backlog = uint32_t{128};
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
constexpr auto wait_for_client_retry_delay = std::chrono::milliseconds{10};
constexpr auto wait_for_client_timeout = std::chrono::seconds{5};
constexpr auto client_probe_interval = std::chrono::seconds{1};
constexpr auto client_probe_timeout = std::chrono::milliseconds{1};
constexpr auto write_timeout = std::chrono::seconds{5};
constexpr auto write_queue_capacity = uint32_t{256};
constexpr auto client_queue_capacity = uint32_t{256};
using WriteQueue = folly::coro::BoundedQueue<Option<chunk_ptr>>;

struct ServeTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  std::optional<located<uint64_t>> max_connections;
  located<ir::pipeline> printer;
};

class ServeTcp final : public Operator<table_slice, void> {
public:
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
    write_task_ = ctx.spawn_task(write_loop(ctx.dh()));
    ctx.spawn_task(accept_loop(ctx.dh()));
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      request_stop();
      if (write_task_) {
        co_await write_task_->join();
        write_task_ = None{};
      }
      co_return;
    }
    auto sub = co_await ctx.spawn_sub(sub_key_, std::move(pipeline),
                                      tag_v<table_slice>);
    TENZIR_UNUSED(sub);
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_) {
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

  auto process_sub(SubKeyView, table_slice, OpCtx& ctx) -> Task<void> override {
    diagnostic::error("subpipeline for `serve_tcp` must return bytes")
      .primary(args_.printer.source.subloc(0, 1))
      .emit(ctx);
    request_stop();
    co_return;
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (done_ or not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await write_queue_->enqueue(Option{std::move(chunk)});
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_return FinalizeBehavior::done;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    if (write_task_) {
      if (not done_) {
        co_await write_queue_->enqueue(None{});
      }
      co_await write_task_->join();
      write_task_ = None{};
    }
    request_stop();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  using Client = Box<folly::coro::Transport>;
  using ClientQueue = folly::coro::BoundedQueue<Client>;

  auto request_stop() -> void {
    if (done_) {
      return;
    }
    done_ = true;
    if (server_) {
      server_->close();
    }
    TENZIR_UNUSED(write_queue_->try_enqueue(None{}));
  }

  auto drain_new_clients(std::vector<Client>& clients) -> void {
    while (auto client = client_queue_->try_dequeue()) {
      clients.push_back(std::move(*client));
    }
  }

  auto write_chunk_to_clients(chunk_ptr chunk, std::vector<Client>& clients,
                              diagnostic_handler& dh) -> Task<void> {
    if (not chunk or chunk->size() == 0 or clients.empty()) {
      co_return;
    }
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    for (auto it = clients.begin(); it != clients.end();) {
      auto peer = (*it)->getPeerAddress().describe();
      try {
        co_await folly::coro::timeout(
          folly::coro::co_withExecutor(evb_, (*it)->write(data)),
          write_timeout);
        ++it;
      } catch (folly::OperationCancelled const&) {
        // Cancellation is part of normal shutdown.
        co_return;
      } catch (folly::FutureTimeout const& ex) {
        // Slow or stalled clients are expected in production; drop only the
        // affected connection and continue serving others.
        diagnostic::warning("failed to write to client {}", peer)
          .primary(args_.endpoint.source)
          .note("reason: {}", ex.what())
          .note(
            "dropping this client and continuing with remaining connections")
          .emit(dh);
        active_clients_->fetch_sub(1, std::memory_order_relaxed);
        it = clients.erase(it);
      } catch (folly::AsyncSocketException const& ex) {
        // Socket write failures are client-scoped; drop this client and keep
        // the listener/writer running.
        diagnostic::warning("failed to write to client {}", peer)
          .primary(args_.endpoint.source)
          .note("reason: {}", ex.what())
          .note(
            "dropping this client and continuing with remaining connections")
          .emit(dh);
        active_clients_->fetch_sub(1, std::memory_order_relaxed);
        it = clients.erase(it);
      }
    }
  }

  auto probe_clients(std::vector<Client>& clients) -> Task<void> {
    for (auto it = clients.begin(); it != clients.end();) {
      auto peer = (*it)->getPeerAddress().describe();
      auto disconnected = false;
      try {
        folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
        auto bytes = co_await (*it)->read(buf, 1, 1, client_probe_timeout);
        disconnected = bytes == 0;
      } catch (folly::OperationCancelled const&) {
        // Cancellation is part of normal shutdown.
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        if (ex.getType() == folly::AsyncSocketException::TIMED_OUT) {
          ++it;
          continue;
        }
        TENZIR_DEBUG("dropping client {} after probe error: {}", peer,
                     ex.what());
        disconnected = true;
      }
      if (disconnected) {
        TENZIR_DEBUG("dropping disconnected client {}", peer);
        active_clients_->fetch_sub(1, std::memory_order_relaxed);
        it = clients.erase(it);
      } else {
        ++it;
      }
    }
  }

  auto write_loop(diagnostic_handler& dh) -> Task<void> {
    auto clients = std::vector<Client>{};
    auto client_wait_deadline = Option<std::chrono::steady_clock::time_point>{};
    auto next_client_probe_at
      = std::chrono::steady_clock::now() + client_probe_interval;
    while (true) {
      auto next = Option<chunk_ptr>{};
      auto now = std::chrono::steady_clock::now();
      auto wait_for_input
        = std::max(std::chrono::milliseconds{0},
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                     next_client_probe_at - now));
      if (wait_for_input == std::chrono::milliseconds{0}) {
        co_await probe_clients(clients);
        next_client_probe_at
          = std::chrono::steady_clock::now() + client_probe_interval;
        continue;
      }
      auto probe_due = false;
      try {
        next = co_await folly::coro::timeout(write_queue_->dequeue(),
                                             wait_for_input);
      } catch (folly::FutureTimeout const&) {
        probe_due = true;
      }
      if (probe_due) {
        co_await probe_clients(clients);
        next_client_probe_at
          = std::chrono::steady_clock::now() + client_probe_interval;
        continue;
      }
      if (done_ or not next) {
        break;
      }
      auto chunk = std::move(*next);
      if (not chunk or chunk->size() == 0) {
        continue;
      }
      drain_new_clients(clients);
      if (clients.empty()) {
        if (not client_wait_deadline) {
          client_wait_deadline
            = std::chrono::steady_clock::now() + wait_for_client_timeout;
        }
        while (clients.empty() and not done_) {
          drain_new_clients(clients);
          if (not clients.empty()) {
            client_wait_deadline = None{};
            break;
          }
          if (client_wait_deadline
              and std::chrono::steady_clock::now() >= *client_wait_deadline) {
            break;
          }
          co_await folly::coro::sleep(wait_for_client_retry_delay);
        }
      } else {
        client_wait_deadline = None{};
      }
      if (clients.empty()) {
        continue;
      }
      client_wait_deadline = None{};
      co_await write_chunk_to_clients(std::move(chunk), clients, dh);
      next_client_probe_at
        = std::chrono::steady_clock::now() + client_probe_interval;
    }
    if (not clients.empty()) {
      active_clients_->fetch_sub(detail::narrow<uint64_t>(clients.size()),
                                 std::memory_order_relaxed);
    }
    request_stop();
    co_return;
  }

  auto accept_loop(diagnostic_handler& dh) -> Task<void> {
    TENZIR_ASSERT(server_);
    while (not done_) {
      auto accept_error = Option<std::string>{};
      auto counted_client = false;
      try {
        auto transport
          = co_await folly::coro::co_withExecutor(evb_, server_->accept());
        auto boxed
          = Box<folly::coro::Transport>::from_unique_ptr(std::move(transport));
        if (tls_context_) {
          co_await upgrade_transport_to_tls_server(boxed, tls_context_);
        }
        auto peer = boxed->getPeerAddress().describe();
        if (active_clients_->load(std::memory_order_relaxed)
            >= max_connections_) {
          diagnostic::warning(
            "connection rejected: maximum number of connections reached")
            .primary(args_.endpoint.source)
            .note("peer: {}", peer)
            .note("max_connections: {}", max_connections_)
            .hint("increase `max_connections` if this level of concurrency is "
                  "expected")
            .emit(dh);
          continue;
        }
        auto client = std::move(boxed);
        if (done_) {
          co_return;
        }
        active_clients_->fetch_add(1, std::memory_order_relaxed);
        counted_client = true;
        co_await client_queue_->enqueue(std::move(client));
        continue;
      } catch (folly::OperationCancelled const&) {
        // Cancellation is part of normal shutdown.
        if (counted_client) {
          active_clients_->fetch_sub(1, std::memory_order_relaxed);
        }
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        // Accept/TLS handshake failures are per-connection network errors; keep
        // the listener alive and continue accepting new clients.
        if (counted_client) {
          active_clients_->fetch_sub(1, std::memory_order_relaxed);
        }
        if (done_) {
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
      if (done_) {
        co_return;
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
  Box<WriteQueue> write_queue_{std::in_place, write_queue_capacity};
  Box<ClientQueue> client_queue_{std::in_place, client_queue_capacity};
  Option<AsyncHandle<void>> write_task_;
  Box<std::atomic<uint64_t>> active_clients_{std::in_place, uint64_t{0}};
  uint64_t max_connections_ = 128;
  bool done_ = false;
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
