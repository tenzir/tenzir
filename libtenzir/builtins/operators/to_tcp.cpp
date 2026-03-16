//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
// Leave room for many queued payloads while still applying backpressure when
// the remote peer falls behind or reconnects take time.
constexpr auto message_queue_capacity = uint32_t{1_Ki};

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

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (done_ or not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await message_queue_->enqueue(std::move(chunk));
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* chunk = result.try_as<chunk_ptr>();
    if (not chunk) {
      co_return;
    }
    if (not *chunk) {
      // A null chunk marks end-of-stream after all buffered payloads.
      request_stop();
      close_current_transport();
      co_return;
    }
    if (done_ or (*chunk)->size() == 0) {
      co_return;
    }
    co_await write_chunk(*chunk, ctx.dh());
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (done_) {
      close_current_transport();
      co_return FinalizeBehavior::done;
    }
    if (auto sub = ctx.get_sub(make_view(sub_key_))) {
      auto pipeline = as<OpenPipeline<table_slice>>(*sub);
      co_await pipeline.close();
      co_return FinalizeBehavior::continue_;
    }
    request_stop();
    close_current_transport();
    co_return FinalizeBehavior::done;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    if (done_) {
      co_return;
    }
    co_await message_queue_->enqueue(chunk_ptr{});
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  using MessageQueue = folly::coro::BoundedQueue<chunk_ptr>;

  static auto close_transport(Box<folly::coro::Transport> transport) -> void {
    auto* evb = transport->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([transport = std::move(transport)]() mutable {
      transport->close();
    });
  }

  auto close_current_transport() -> void {
    if (transport_) {
      auto old_transport = std::move(*transport_);
      transport_ = None{};
      close_transport(std::move(old_transport));
    }
  }

  auto ensure_connected(diagnostic_handler& dh) -> Task<void> {
    while (not done_) {
      if (transport_) {
        co_return;
      }
      auto connect_error = Option<std::string>{};
      try {
        transport_ = Box<folly::coro::Transport>{co_await connect_tcp_client(
          evb_, address_,
          std::chrono::duration_cast<std::chrono::milliseconds>(connect_timeout),
          tls_context_, host_)};
        reconnect_backoff_ = connect_initial_backoff;
        TENZIR_DEBUG("to_tcp: connected to {}", address_.describe());
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        if (done_) {
          co_return;
        }
        connect_error = ex.what();
      }
      auto backoff = reconnect_backoff_;
      reconnect_backoff_
        = std::min(reconnect_backoff_ * 2, connect_max_backoff);
      // TODO: Surface connect retries and failures as metrics in a follow-up
      // that covers all TCP operators.
      diagnostic::warning("failed to connect to {}", address_.describe())
        .primary(args_.endpoint.source)
        .note("reason: {}", *connect_error)
        .hint("ensure a TCP server is listening on this endpoint")
        .emit(dh);
      co_await folly::coro::sleep(backoff);
    }
  }

  auto write_chunk(chunk_ptr const& chunk, diagnostic_handler& dh)
    -> Task<void> {
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    while (not done_) {
      co_await ensure_connected(dh);
      if (done_ or not transport_) {
        co_return;
      }
      auto write_error = Option<std::string>{};
      try {
        co_await folly::coro::co_withExecutor(evb_, (**transport_).write(data));
        reconnect_backoff_ = connect_initial_backoff;
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        write_error = ex.what();
      }
      // TODO: Surface routine TCP write failures and reconnects as metrics
      // in a follow-up that covers all TCP operators.
      diagnostic::warning("failed to write to {}", address_.describe())
        .primary(args_.endpoint.source)
        .note("reason: {}", *write_error)
        .note("retrying after reconnect")
        .emit(dh);
      close_current_transport();
    }
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
  mutable Box<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Box<folly::coro::Transport>> transport_;
  std::chrono::milliseconds reconnect_backoff_ = connect_initial_backoff;
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
