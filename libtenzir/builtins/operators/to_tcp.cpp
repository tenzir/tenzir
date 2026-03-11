//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/mutex.hpp>
#include <tenzir/async/tcp.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
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
#include <folly/io/coro/Transport.h>

#include <algorithm>
#include <atomic>
#include <memory>

namespace tenzir::plugins::to_tcp {

namespace {

using namespace tenzir::si_literals;

// Give remote endpoints long enough for a normal TCP plus TLS setup while
// still surfacing unavailable servers quickly to the retry loop.
constexpr auto connect_timeout = std::chrono::seconds{5};
// Reconnect almost immediately after the first failure so short races during
// fixture startup or service restarts do not stall the pipeline.
constexpr auto connect_initial_backoff = std::chrono::milliseconds{100};
// Cap retries at five seconds to avoid hammering broken endpoints while still
// keeping recovery reasonably quick once the peer comes back.
constexpr auto connect_max_backoff = std::chrono::milliseconds{5_k};
// Bound a single write attempt so a stalled peer stops the pipeline instead of
// letting output bytes pile up behind an indefinitely blocked socket.
constexpr auto write_timeout = std::chrono::seconds{5};
// The control queue only carries wakeups and reconnect requests, so a small
// fixed capacity is sufficient while still tolerating short bursts.
constexpr auto control_queue_capacity = uint32_t{64};

struct ToTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  located<ir::pipeline> printer;
};

class ToTcp final : public Operator<table_slice, void> {
public:
  struct EnsureConnected {};
  struct Wakeup {};
  using Message = variant<EnsureConnected, Wakeup>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  explicit ToTcp(ToTcpArgs args) : args_{std::move(args)} {
    auto ep = to<struct endpoint>(args_.endpoint.inner);
    TENZIR_ASSERT(ep);
    TENZIR_ASSERT(ep->port);
    TENZIR_ASSERT(not ep->host.empty());
    address_.setFromHostPort(ep->host, ep->port->number());
    host_ = ep->host;
    if (args_.tls) {
      tls_ = tls_options{*args_.tls, {.is_server = false}};
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
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      request_stop();
      co_return;
    }
    auto sub = co_await ctx.spawn_sub(sub_key_, std::move(pipeline),
                                      tag_v<table_slice>);
    TENZIR_UNUSED(sub);
    request_connect();
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_->load()) {
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
    if (done_->load() or not chunk or chunk->size() == 0) {
      co_return;
    }
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    {
      auto guard = co_await transport_mutex_->lock();
      if (done_->load()) {
        co_return;
      }
      if (not transport_) {
        request_connect();
      }
    }
    co_await ensure_connected(ctx);
    auto old_transport = Option<Box<folly::coro::Transport>>{};
    {
      auto guard = co_await transport_mutex_->lock();
      if (done_->load()) {
        co_return;
      }
      if (not transport_) {
        diagnostic::warning("connection to {} is not established",
                            address_.describe())
          .primary(args_.endpoint.source)
          .note("stopping to avoid dropping output bytes")
          .hint("restart once the TCP endpoint is reachable")
          .emit(ctx);
        request_stop();
        co_return;
      }
      try {
        co_await folly::coro::timeout(
          folly::coro::co_withExecutor(evb_, (*transport_)->write(data)),
          write_timeout);
        reconnect_backoff_ = connect_initial_backoff;
        co_return;
      } catch (folly::FutureTimeout const& ex) {
        if (done_->load()) {
          co_return;
        }
        diagnostic::warning("failed to write to {}", address_.describe())
          .primary(args_.endpoint.source)
          .note("reason: {}", ex.what())
          .note("stopping to avoid dropping output bytes")
          .emit(ctx);
        old_transport = std::move(transport_);
        transport_ = None{};
      } catch (folly::AsyncSocketException const& ex) {
        if (done_->load()) {
          co_return;
        }
        diagnostic::warning("failed to write to {}", address_.describe())
          .primary(args_.endpoint.source)
          .note("reason: {}", ex.what())
          .note("stopping to avoid dropping output bytes")
          .emit(ctx);
        old_transport = std::move(transport_);
        transport_ = None{};
      }
    }
    if (old_transport) {
      close_transport(std::move(*old_transport));
      request_stop();
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_->load()) {
      co_return {};
    }
    co_return co_await control_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    if (done_->load()) {
      co_return;
    }
    auto* message = result.try_as<Message>();
    if (not message) {
      co_return;
    }
    if (std::holds_alternative<EnsureConnected>(*message)) {
      connect_scheduled_->store(false);
      co_await ensure_connected(ctx);
    }
    co_return;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_await close_current_transport();
    co_return FinalizeBehavior::done;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    request_stop();
    co_await close_current_transport();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_->load() ? OperatorState::done : OperatorState::unspecified;
  }

private:
  static auto close_transport(Box<folly::coro::Transport> transport) -> void {
    auto* evb = transport->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([transport = std::move(transport)]() mutable {
      transport->close();
    });
  }

  auto close_current_transport() -> Task<void> {
    auto old_transport = Option<Box<folly::coro::Transport>>{};
    {
      auto guard = co_await transport_mutex_->lock();
      old_transport = std::move(transport_);
      transport_ = None{};
    }
    if (old_transport) {
      close_transport(std::move(*old_transport));
    }
    co_return;
  }

  auto ensure_connected(OpCtx& ctx) -> Task<void> {
    {
      auto guard = co_await transport_mutex_->lock();
      if (done_->load() or transport_) {
        co_return;
      }
    }
    auto connect_error = Option<std::string>{};
    try {
      auto boxed = Box<folly::coro::Transport>{co_await connect_tcp_client(
        evb_, address_,
        std::chrono::duration_cast<std::chrono::milliseconds>(connect_timeout),
        tls_context_, host_)};
      auto guard = co_await transport_mutex_->lock();
      if (done_->load() or transport_) {
        close_transport(std::move(boxed));
        co_return;
      }
      transport_ = std::move(boxed);
      reconnect_backoff_ = connect_initial_backoff;
      TENZIR_DEBUG("to_tcp ensure_connected: connected to {}",
                   address_.describe());
      co_return;
    } catch (folly::AsyncSocketException const& ex) {
      // Connect/TLS handshake failures are per-attempt network errors; retry
      // asynchronously with backoff instead of blocking this task forever.
      if (done_->load()) {
        co_return;
      }
      connect_error = ex.what();
    }
    if (connect_error) {
      auto backoff = connect_initial_backoff;
      {
        auto guard = co_await transport_mutex_->lock();
        if (done_->load()) {
          co_return;
        }
        backoff = reconnect_backoff_;
        reconnect_backoff_
          = std::min(reconnect_backoff_ * 2, connect_max_backoff);
      }
      diagnostic::warning("failed to connect to {}", address_.describe())
        .primary(args_.endpoint.source)
        .note("reason: {}", *connect_error)
        .hint("ensure a TCP server is listening on this endpoint")
        .emit(ctx);
      ctx.spawn_task([this, backoff]() -> Task<void> {
        co_await folly::coro::sleep(backoff);
        request_connect();
      }());
    }
  }

  auto request_stop() -> void {
    if (done_->exchange(true)) {
      return;
    }
    connect_scheduled_->store(false);
    TENZIR_UNUSED(control_queue_->try_enqueue(Wakeup{}));
  }

  auto request_connect() -> void {
    if (done_->load()) {
      return;
    }
    auto expected = false;
    if (not connect_scheduled_->compare_exchange_strong(expected, true)) {
      return;
    }
    if (not control_queue_->try_enqueue(EnsureConnected{})) {
      connect_scheduled_->store(false);
    }
  }

  ToTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  std::string host_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  Box<RawMutex> transport_mutex_{std::in_place};
  Option<Box<folly::coro::Transport>> transport_;
  std::chrono::milliseconds reconnect_backoff_ = connect_initial_backoff;
  mutable Box<MessageQueue> control_queue_{std::in_place,
                                           control_queue_capacity};
  Box<std::atomic_bool> connect_scheduled_{std::in_place, false};
  Box<std::atomic_bool> done_{std::in_place, false};
};

class ToTcpPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToTcpArgs, ToTcp>{};
    auto endpoint_arg = d.positional("endpoint", &ToTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &ToTcpArgs::tls);
    auto printer_arg = d.pipeline(&ToTcpArgs::printer);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_str, ctx.get(endpoint_arg));
      auto ep = to<struct endpoint>(endpoint_str.inner);
      auto loc = ctx.get_location(endpoint_arg).value_or(location::unknown);
      if (not ep) {
        diagnostic::error("failed to parse endpoint").primary(loc).emit(ctx);
      } else if (not ep->port) {
        diagnostic::error("port number is required").primary(loc).emit(ctx);
      } else if (ep->host.empty()) {
        diagnostic::error("host is required").primary(loc).emit(ctx);
      }
      if (auto tls_val = ctx.get(tls_arg)) {
        auto tls_opts = tls_options{*tls_val, {.is_server = false}};
        if (auto valid = tls_opts.validate(ctx); not valid) {
          return {};
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::to_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_tcp::ToTcpPlugin)
