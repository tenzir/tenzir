//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
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
#include <folly/coro/Sleep.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/Transport.h>

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

struct ToTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  located<ir::pipeline> printer;
};

class ToTcp final : public Operator<table_slice, void> {
public:
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

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or not chunk or chunk->size() == 0) {
      co_return;
    }
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    while (not done_) {
      co_await ensure_connected(ctx);
      if (done_) {
        co_return;
      }
      auto transport = Option<Transport>{};
      {
        auto guard = co_await state_->lock();
        if (done_) {
          co_return;
        }
        TENZIR_ASSERT(guard->transport);
        transport = guard->transport;
      }
      TENZIR_ASSERT(transport);
      auto write_error = Option<std::string>{};
      try {
        co_await folly::coro::co_withExecutor(evb_, (**transport).write(data));
      } catch (folly::AsyncSocketException const& ex) {
        write_error = ex.what();
      }
      if (not write_error) {
        auto guard = co_await state_->lock();
        guard->reconnect_backoff = connect_initial_backoff;
        co_return;
      }
      auto guard = co_await state_->lock();
      auto* transport_ptr = &**transport;
      if (guard->transport and &**guard->transport == transport_ptr) {
        auto old_transport = std::move(guard->transport);
        guard->transport = None{};
        // TODO: Surface routine TCP write failures and reconnects as metrics
        // in a follow-up that covers all TCP operators.
        diagnostic::warning("failed to write to {}", address_.describe())
          .primary(args_.endpoint.source)
          .note("reason: {}", *write_error)
          .note("retrying after reconnect")
          .emit(ctx);
        close_transport(std::move(*old_transport));
      }
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await wait_forever();
    TENZIR_UNREACHABLE();
  }

  auto process_task(Any, OpCtx&) -> Task<void> override {
    TENZIR_UNREACHABLE();
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (done_) {
      co_await close_current_transport();
      co_return FinalizeBehavior::done;
    }
    if (auto sub = ctx.get_sub(make_view(sub_key_))) {
      auto pipeline = as<OpenPipeline<table_slice>>(*sub);
      co_await pipeline.close();
      co_return FinalizeBehavior::continue_;
    }
    request_stop();
    co_await close_current_transport();
    co_return FinalizeBehavior::done;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    request_stop();
    co_await close_current_transport();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  using Transport = Arc<folly::coro::Transport>;

  struct State {
    Option<Transport> transport;
    std::chrono::milliseconds reconnect_backoff = connect_initial_backoff;
  };

  static auto close_transport(Transport transport) -> void {
    auto* evb = transport->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([transport = std::move(transport)]() mutable {
      transport->close();
    });
  }

  auto close_current_transport() -> Task<void> {
    auto old_transport = Option<Transport>{};
    {
      auto guard = co_await state_->lock();
      old_transport = std::move(guard->transport);
      guard->transport = None{};
    }
    if (old_transport) {
      close_transport(std::move(*old_transport));
    }
    co_return;
  }

  auto ensure_connected(OpCtx& ctx) -> Task<void> {
    while (not done_) {
      {
        auto guard = co_await state_->lock();
        if (guard->transport) {
          co_return;
        }
      }
      auto connect_error = Option<std::string>{};
      try {
        auto transport = Transport{co_await connect_tcp_client(
          evb_, address_,
          std::chrono::duration_cast<std::chrono::milliseconds>(connect_timeout),
          tls_context_, host_)};
        auto guard = co_await state_->lock();
        if (done_ or guard->transport) {
          close_transport(std::move(transport));
          co_return;
        }
        guard->transport = std::move(transport);
        guard->reconnect_backoff = connect_initial_backoff;
        TENZIR_DEBUG("to_tcp ensure_connected: connected to {}",
                     address_.describe());
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        if (done_) {
          co_return;
        }
        connect_error = ex.what();
      }
      auto backoff = connect_initial_backoff;
      {
        auto guard = co_await state_->lock();
        if (done_) {
          co_return;
        }
        backoff = guard->reconnect_backoff;
        guard->reconnect_backoff
          = std::min(guard->reconnect_backoff * 2, connect_max_backoff);
      }
      // TODO: Surface connect retries and failures as metrics in a follow-up
      // that covers all TCP operators.
      diagnostic::warning("failed to connect to {}", address_.describe())
        .primary(args_.endpoint.source)
        .note("reason: {}", *connect_error)
        .hint("ensure a TCP server is listening on this endpoint")
        .emit(ctx);
      co_await folly::coro::sleep(backoff);
    }
    co_return;
  }

  auto request_stop() -> void {
    done_ = true;
  }

  ToTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  std::string host_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  Box<Mutex<State>> state_{std::in_place, State{}};
  bool done_ = false;
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
