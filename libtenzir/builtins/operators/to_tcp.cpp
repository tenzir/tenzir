//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/stream.hpp>
#include <tenzir/async/stream_to.hpp>
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
#include <tenzir/socket.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>
#include <memory>

namespace tenzir::plugins::to_tcp {

namespace {

constexpr auto connect_timeout = std::chrono::seconds{5};

struct TcpTo {
  struct Args {
    located<std::string> endpoint;
    Option<located<data>> tls;
    Option<located<uint64_t>> max_retry_count;
    located<ir::pipeline> printer;
  };

  explicit TcpTo(Args args) : args_{std::move(args)} {
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

  auto prepare(OpCtx& ctx) -> Task<bool> {
    if (tls_) {
      auto resolved = tls_->resolve(ctx.actor_system().config(), ctx);
      if (not resolved) {
        co_return false;
      }
      if (resolved->tls.inner) {
        auto context = resolved->make_folly_ssl_context(ctx);
        if (not context) {
          co_return false;
        }
        tls_context_ = std::move(*context);
      }
    }
    co_return true;
  }

  auto printer() -> located<ir::pipeline>& {
    return args_.printer;
  }

  auto max_retry_count() const -> Option<located<uint64_t>> const& {
    return args_.max_retry_count;
  }

  auto events_metric_label() const -> MetricsLabel {
    return {"operator", "to_tcp"};
  }

  auto bytes_metric_label_before_connect() const -> Option<MetricsLabel> {
    return None{};
  }

  auto bytes_metric_label_after_connect(
    folly::coro::Transport const& transport) const -> Option<MetricsLabel> {
    auto peer_addr = transport.getPeerAddress();
    return MetricsLabel{
      "peer_ip",
      MetricsLabel::FixedString::truncate(peer_addr.getAddressStr()),
    };
  }

  auto connect(folly::EventBase* evb) -> Task<folly::coro::Transport> {
    TENZIR_DEBUG("to_tcp: connecting to {}", address_.describe());
    auto transport = co_await connect_tcp_client(
      evb, address_,
      std::chrono::duration_cast<std::chrono::milliseconds>(connect_timeout),
      tls_context_, host_);
    TENZIR_DEBUG("to_tcp: connected to {}", address_.describe());
    co_return transport;
  }

  auto emit_connect_error(folly::AsyncSocketException const& ex,
                          uint32_t max_retry_count,
                          diagnostic_handler& dh) const -> void {
    auto diag
      = diagnostic::error("failed to connect to {}: {}", address_.describe(),
                          describe_socket_error(ex))
          .primary(args_.endpoint.source)
          .note("gave up after {} {}", max_retry_count,
                max_retry_count == 1 ? "retry" : "retries")
          .hint("ensure a TCP server is listening on this endpoint");
    add_tls_client_diagnostic_hints(std::move(diag), is_tls_enabled()).emit(dh);
  }

  auto emit_connect_warning(folly::AsyncSocketException const& ex,
                            diagnostic_handler& dh) const -> void {
    auto diag
      = diagnostic::warning("failed to connect to {}: {}", address_.describe(),
                            describe_socket_error(ex))
          .primary(args_.endpoint.source)
          .hint("ensure a TCP server is listening on this endpoint");
    dh.emit(
      add_tls_client_diagnostic_hints(std::move(diag), is_tls_enabled()).done());
  }

  auto emit_write_warning(folly::AsyncSocketException const& ex,
                          diagnostic_handler& dh) const -> void {
    diagnostic::warning("failed to write to {}", address_.describe())
      .primary(args_.endpoint.source)
      .note("reason: {}", ex.what())
      .note("retrying after reconnect")
      .emit(dh);
  }

private:
  auto is_tls_enabled() const -> bool {
    return tls_context_ != nullptr;
  }

  Args args_;
  folly::SocketAddress address_;
  std::string host_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
};

using ToTcpArgs = TcpTo::Args;
using ToTcp = StreamTo<TcpTo>;

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
