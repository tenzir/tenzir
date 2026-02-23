//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/notify.hpp>
#include <tenzir/async/tls.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/coro/Mutex.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/Transport.h>

#include <algorithm>

namespace tenzir::plugins::to_tcp {

namespace {

constexpr auto connect_timeout = std::chrono::seconds{5};
constexpr auto connect_initial_backoff = std::chrono::milliseconds{100};
constexpr auto connect_max_backoff = std::chrono::milliseconds{5000};
constexpr auto write_timeout = std::chrono::seconds{5};

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
        done_ = true;
        co_return;
      }
      tls_context_ = std::move(*context);
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto pipeline_copy = args_.printer.inner;
    if (not pipeline_copy.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      done_ = true;
      co_return;
    }
    auto sub = co_await ctx.spawn_sub(sub_key_, std::move(pipeline_copy),
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
      done_ = true;
      co_return;
    }
    auto pipeline = as<OpenPipeline<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      done_ = true;
    }
  }

  auto process_sub(SubKeyView, table_slice, OpCtx& ctx) -> Task<void> override {
    diagnostic::error("subpipeline for `to_tcp` must return bytes")
      .primary(args_.printer.source.subloc(0, 1))
      .emit(ctx);
    done_ = true;
    co_return;
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or not chunk or chunk->size() == 0) {
      co_return;
    }
    auto lock = co_await write_mutex_->co_scoped_lock();
    while (true) {
      if (not transport_) {
        co_await connect(ctx.dh());
      }
      TENZIR_ASSERT(transport_);
      auto data = folly::ByteRange{
        reinterpret_cast<unsigned char const*>(chunk->data()),
        chunk->size(),
      };
      try {
        co_await folly::coro::timeout(
          folly::coro::co_withExecutor(evb_, (*transport_)->write(data)),
          write_timeout);
        reconnect_backoff_ = connect_initial_backoff;
        co_return;
      } catch (std::exception const& ex) {
        diagnostic::warning("to_tcp: failed to write to {}: {}",
                            address_.describe(), ex.what())
          .emit(ctx);
        transport_.reset();
      }
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (sub) {
      auto pipeline = as<OpenPipeline<table_slice>>(*sub);
      co_await pipeline.close();
      co_await sub_finished_->wait();
    }
    {
      auto lock = co_await write_mutex_->co_scoped_lock();
      transport_.reset();
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    sub_finished_->notify_one();
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  auto connect(diagnostic_handler& dh) -> Task<void> {
    while (true) {
      auto connect_error = std::optional<std::string>{};
      try {
        auto transport = co_await folly::coro::co_withExecutor(
          evb_, folly::coro::Transport::newConnectedSocket(evb_, address_,
                                                           connect_timeout));
        auto boxed = Box<folly::coro::Transport>{std::move(transport)};
        if (tls_context_) {
          co_await upgrade_transport_to_tls_client(boxed, tls_context_, host_);
        }
        transport_ = std::move(boxed);
        reconnect_backoff_ = connect_initial_backoff;
        co_return;
      } catch (std::exception const& ex) {
        connect_error = ex.what();
      }
      if (connect_error) {
        diagnostic::warning("to_tcp: failed to connect to {}: {}",
                            address_.describe(), *connect_error)
          .emit(dh);
      }
      auto backoff = reconnect_backoff_;
      reconnect_backoff_
        = std::min(reconnect_backoff_ * 2, connect_max_backoff);
      co_await folly::coro::sleep(backoff);
    }
  }

  ToTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  std::string host_;
  std::optional<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::optional<Box<folly::coro::Transport>> transport_;
  std::chrono::milliseconds reconnect_backoff_ = connect_initial_backoff;
  std::shared_ptr<folly::coro::Mutex> write_mutex_
    = std::make_shared<folly::coro::Mutex>();
  std::shared_ptr<Notify> sub_finished_ = std::make_shared<Notify>();
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
    d.validate([=](ValidateCtx& ctx) -> Empty {
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
