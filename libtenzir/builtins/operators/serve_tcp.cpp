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
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <algorithm>

namespace tenzir::plugins::serve_tcp {

namespace {

constexpr auto listen_backlog = uint32_t{128};
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
constexpr auto write_timeout = std::chrono::seconds{5};

struct ServeTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
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
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, listen_backlog);
    ctx.spawn_task(accept_loop(ctx.dh()));
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
    diagnostic::error("subpipeline for `serve_tcp` must return bytes")
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
    auto snapshot = std::vector<std::shared_ptr<folly::coro::Transport>>{};
    {
      auto lock = co_await clients_mutex_->co_scoped_lock();
      snapshot = clients_;
    }
    auto failed = std::vector<std::shared_ptr<folly::coro::Transport>>{};
    for (auto& client : snapshot) {
      auto data = folly::ByteRange{
        reinterpret_cast<unsigned char const*>(chunk->data()),
        chunk->size(),
      };
      try {
        co_await folly::coro::timeout(
          folly::coro::co_withExecutor(evb_, client->write(data)),
          write_timeout);
      } catch (std::exception const& ex) {
        diagnostic::warning(
          "serve_tcp: dropping client after write failure: {}", ex.what())
          .emit(ctx);
        failed.push_back(client);
      }
    }
    if (failed.empty()) {
      co_return;
    }
    auto lock = co_await clients_mutex_->co_scoped_lock();
    std::erase_if(clients_, [&](auto const& client) {
      return std::ranges::find(failed, client) != failed.end();
    });
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (sub) {
      auto pipeline = as<OpenPipeline<table_slice>>(*sub);
      co_await pipeline.close();
      co_await sub_finished_->wait();
    }
    {
      auto lock = co_await clients_mutex_->co_scoped_lock();
      clients_.clear();
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
  auto accept_loop(diagnostic_handler& dh) -> Task<void> {
    TENZIR_ASSERT(server_);
    while (true) {
      auto accept_error = std::optional<std::string>{};
      try {
        auto transport
          = co_await folly::coro::co_withExecutor(evb_, server_->accept());
        auto boxed
          = Box<folly::coro::Transport>::from_unique_ptr(std::move(transport));
        if (tls_context_) {
          co_await upgrade_transport_to_tls_server(boxed, tls_context_);
        }
        auto client
          = std::make_shared<folly::coro::Transport>(std::move(*boxed));
        auto lock = co_await clients_mutex_->co_scoped_lock();
        clients_.push_back(std::move(client));
      } catch (folly::OperationCancelled) {
        co_return;
      } catch (std::exception const& ex) {
        if (done_) {
          co_return;
        }
        accept_error = ex.what();
      }
      if (accept_error) {
        diagnostic::warning("serve_tcp: failed to accept connection on {}: {}",
                            address_.describe(), *accept_error)
          .emit(dh);
      }
      co_await folly::coro::sleep(accept_retry_delay);
    }
  }

  ServeTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  std::optional<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  std::vector<std::shared_ptr<folly::coro::Transport>> clients_;
  std::shared_ptr<folly::coro::Mutex> clients_mutex_
    = std::make_shared<folly::coro::Mutex>();
  std::shared_ptr<Notify> sub_finished_ = std::make_shared<Notify>();
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
