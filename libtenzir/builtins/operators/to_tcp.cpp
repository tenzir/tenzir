//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "detail/stream.hpp"
#include "tenzir/async.hpp"

#include <tenzir/async/tcp.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/String.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>
#include <memory>

namespace tenzir::plugins::to_tcp {

namespace {

using namespace tenzir::si_literals;

// Leave room for some queued payloads while still applying backpressure when
// the remote peer falls behind or reconnects take time.
constexpr auto message_queue_capacity = uint32_t{10};

struct ToTcpArgs {
  located<std::string> endpoint;
  Option<located<data>> tls;
  Option<located<uint64_t>> max_retry_count;
  located<ir::pipeline> printer;
};

class ToTcp final : public Operator<table_slice, void> {
public:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit ToTcp(ToTcpArgs args) : args_{std::move(args)} {
    auto ep = to<Endpoint>(args_.endpoint.inner);
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
    if (tls_) {
      auto resolved = tls_->resolve(ctx.actor_system().config(), ctx);
      if (not resolved) {
        finish();
        co_return;
      }
      tls_enabled_ = resolved->tls.inner;
      if (tls_enabled_) {
        auto context = resolved->make_folly_ssl_context(ctx);
        if (not context) {
          finish();
          co_return;
        }
        tls_context_ = std::move(*context);
      }
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      finish();
      co_return;
    }
    events_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_tcp"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    co_await ctx.spawn_sub<table_slice>(sub_key_, std::move(pipeline));
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (not sub) {
      finish();
      co_return;
    }
    auto const rows = input.rows();
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      finish();
      co_return;
    }
    events_write_counter_.add(rows);
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0) {
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
      finish();
      co_return;
    }
    if (lifecycle_ == Lifecycle::done or (*chunk)->size() == 0) {
      co_return;
    }
    co_await write_chunk(*chunk, ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      close_current_transport();
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      if (auto sub = ctx.get_sub(make_view(sub_key_))) {
        auto& pipeline = as<SubHandle<table_slice>>(*sub);
        co_await pipeline.close();
        co_return FinalizeBehavior::continue_;
      }
      finish();
      co_return FinalizeBehavior::done;
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    ctx.spawn_task([this]() -> Task<void> {
      co_await message_queue_->enqueue(chunk_ptr{});
    });
    co_return;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  using MessageQueue = folly::coro::BoundedQueue<chunk_ptr>;

  auto close_current_transport() -> void {
    if (transport_) {
      auto old_transport = std::move(*transport_);
      transport_ = None{};
      stream_detail::close_transport(std::move(old_transport));
    }
  }

  auto ensure_connected(OpCtx& ctx) -> Task<void> {
    if (lifecycle_ == Lifecycle::done or transport_) {
      co_return;
    }
    auto const max_retry_count
      = args_.max_retry_count
          ? detail::narrow<uint32_t>(args_.max_retry_count->inner)
          : stream_detail::default_connect_max_retry_count;
    auto emit_final_error = [&](folly::AsyncSocketException const& ex) {
      auto diag
        = diagnostic::error("failed to connect to {}: {}", address_.describe(),
                            stream_detail::describe_socket_error(ex))
            .primary(args_.endpoint.source)
            .note("gave up after {} {}", max_retry_count,
                  max_retry_count == 1 ? "retry" : "retries")
            .hint("ensure a TCP server is listening on this endpoint");
      add_tls_client_diagnostic_hints(std::move(diag), is_tls_enabled())
        .emit(ctx.dh());
      finish();
    };
    auto emit_retry_warning = [&](folly::AsyncSocketException const& ex) {
      // TODO: Surface connect retries and failures as metrics in a follow-up
      // that covers all TCP operators.
      auto diag = diagnostic::warning("failed to connect to {}: {}",
                                      address_.describe(),
                                      stream_detail::describe_socket_error(ex))
                    .primary(args_.endpoint.source)
                    .hint("ensure a TCP server is listening on this endpoint");
      ctx.dh().emit(
        add_tls_client_diagnostic_hints(std::move(diag), is_tls_enabled())
          .done());
    };
    auto connect = [this]() -> Task<folly::coro::Transport> {
      TENZIR_DEBUG("to_tcp: connecting to {}", address_.describe());
      co_return co_await connect_tcp_client(
        evb_, address_,
        std::chrono::duration_cast<std::chrono::milliseconds>(
          stream_detail::connect_timeout),
        tls_context_, host_);
    };
    try {
      transport_ = co_await folly::coro::retryWithExponentialBackoff(
        max_retry_count, stream_detail::connect_initial_backoff,
        stream_detail::connect_max_backoff, stream_detail::connect_retry_jitter,
        [this, &connect,
         &emit_retry_warning]() -> Task<folly::coro::Transport> {
          try {
            co_return co_await connect();
          } catch (folly::AsyncSocketException const& ex) {
            if (not args_.max_retry_count) {
              emit_retry_warning(ex);
            }
            throw;
          }
        },
        stream_detail::should_retry_socket);
    } catch (folly::AsyncSocketException const& ex) {
      emit_final_error(ex);
      co_return;
    }
    auto peer_addr = transport_->getPeerAddress();
    bytes_write_counter_ = ctx.make_counter(
      MetricsLabel{"peer_ip", MetricsLabel::FixedString::truncate(
                                peer_addr.getAddressStr())},
      MetricsDirection::write, MetricsVisibility::external_,
      MetricsUnit::bytes);
    TENZIR_DEBUG("to_tcp: connected to {}", address_.describe());
  }

  auto write_chunk(chunk_ptr const& chunk, OpCtx& ctx) -> Task<void> {
    auto data = stream_detail::as_byte_range(chunk);
    while (lifecycle_ != Lifecycle::done) {
      co_await ensure_connected(ctx);
      if (lifecycle_ == Lifecycle::done or not transport_) {
        co_return;
      }
      auto write_error = Option<std::string>{};
      auto* transport_evb = transport_->getEventBase();
      TENZIR_ASSERT(transport_evb);
      try {
        co_await folly::coro::co_withExecutor(transport_evb,
                                              transport_->write(data));
        bytes_write_counter_.add(chunk->size());
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
        .emit(ctx.dh());
      close_current_transport();
    }
  }

  auto finish() -> void {
    lifecycle_ = Lifecycle::done;
    close_current_transport();
  }

  auto is_tls_enabled() const -> bool {
    return tls_enabled_;
  }

  ToTcpArgs args_;
  data sub_key_ = data{int64_t{0}};
  folly::SocketAddress address_;
  std::string host_;
  Option<tls_options> tls_;
  bool tls_enabled_ = false;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  mutable Box<MessageQueue> message_queue_{
    std::in_place,
    message_queue_capacity,
  };
  Option<folly::coro::Transport> transport_;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
  Lifecycle lifecycle_ = Lifecycle::running;
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
    auto max_retry_count_arg
      = d.named("max_retry_count", &ToTcpArgs::max_retry_count);
    auto printer_arg
      = d.pipeline(&ToTcpArgs::printer, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_str, ctx.get(endpoint_arg));
      auto ep = to<Endpoint>(endpoint_str.inner);
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
      if (auto max_retry_count = ctx.get(max_retry_count_arg)) {
        if (max_retry_count->inner > std::numeric_limits<uint32_t>::max()) {
          diagnostic::error("`max_retry_count` must be <= {}",
                            std::numeric_limits<uint32_t>::max())
            .primary(max_retry_count->source)
            .emit(ctx);
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
    return d.invariant_order_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::to_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_tcp::ToTcpPlugin)
