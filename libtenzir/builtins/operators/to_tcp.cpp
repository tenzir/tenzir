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
#include <folly/io/coro/Transport.h>

#include <algorithm>

namespace tenzir::plugins::to_tcp {

namespace {

constexpr auto connect_timeout = std::chrono::seconds{5};
constexpr auto connect_initial_backoff = std::chrono::milliseconds{100};
constexpr auto connect_max_backoff = std::chrono::milliseconds{5000};
constexpr auto write_timeout = std::chrono::seconds{5};
constexpr auto write_queue_capacity = uint32_t{256};
using WriteQueue = folly::coro::BoundedQueue<Option<chunk_ptr>>;

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
    write_task_ = ctx.spawn_task(write_loop(ctx.dh()));
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
    diagnostic::error("subpipeline for `to_tcp` must return bytes")
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
      co_await write_queue_->enqueue(None{});
      co_await write_task_->join();
      write_task_ = None{};
    }
    done_ = true;
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  auto connect(diagnostic_handler& dh) -> Task<void> {
    while (not done_) {
      auto connect_error = Option<std::string>{};
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
      } catch (folly::OperationCancelled const&) {
        // Cancellation is part of normal shutdown.
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        // Connect/TLS handshake failures are per-attempt network errors; keep
        // retrying with backoff instead of failing the whole operator.
        if (done_) {
          co_return;
        }
        connect_error = ex.what();
      }
      if (connect_error) {
        if (done_) {
          co_return;
        }
        diagnostic::warning("failed to connect to {}", address_.describe())
          .primary(args_.endpoint.source)
          .note("reason: {}", *connect_error)
          .hint("ensure a TCP server is listening on this endpoint")
          .emit(dh);
      }
      auto backoff = reconnect_backoff_;
      reconnect_backoff_
        = std::min(reconnect_backoff_ * 2, connect_max_backoff);
      co_await folly::coro::sleep(backoff);
    }
  }

  auto write_loop(diagnostic_handler& dh) -> Task<void> {
    while (true) {
      auto next = co_await write_queue_->dequeue();
      if (not next) {
        break;
      }
      auto chunk = std::move(*next);
      if (not chunk or chunk->size() == 0) {
        continue;
      }
      while (not done_) {
        if (not transport_) {
          co_await connect(dh);
          if (done_ or not transport_) {
            break;
          }
        }
        auto data = folly::ByteRange{
          reinterpret_cast<unsigned char const*>(chunk->data()),
          chunk->size(),
        };
        try {
          co_await folly::coro::timeout(
            folly::coro::co_withExecutor(evb_, (*transport_)->write(data)),
            write_timeout);
          reconnect_backoff_ = connect_initial_backoff;
          break;
        } catch (folly::OperationCancelled const&) {
          // Cancellation is part of normal shutdown.
          co_return;
        } catch (folly::FutureTimeout const& ex) {
          // Slow or stalled peers are expected in production; drop and
          // reconnect this transport only.
          if (done_) {
            break;
          }
          diagnostic::warning("failed to write to {}", address_.describe())
            .primary(args_.endpoint.source)
            .note("reason: {}", ex.what())
            .note("dropping the current connection and retrying")
            .emit(dh);
          transport_ = None{};
        } catch (folly::AsyncSocketException const& ex) {
          // Socket write failures are connection-scoped; reconnect and
          // continue processing queued output.
          if (done_) {
            break;
          }
          diagnostic::warning("failed to write to {}", address_.describe())
            .primary(args_.endpoint.source)
            .note("reason: {}", ex.what())
            .note("dropping the current connection and retrying")
            .emit(dh);
          transport_ = None{};
        }
      }
    }
    transport_ = None{};
    done_ = true;
  }

  auto request_stop() -> void {
    if (done_) {
      return;
    }
    done_ = true;
    TENZIR_UNUSED(write_queue_->try_enqueue(None{}));
  }

  ToTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  std::string host_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  Option<Box<folly::coro::Transport>> transport_;
  std::chrono::milliseconds reconnect_backoff_ = connect_initial_backoff;
  Box<WriteQueue> write_queue_{std::in_place, write_queue_capacity};
  Option<AsyncHandle<void>> write_task_;
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
