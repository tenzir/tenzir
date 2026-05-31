//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/dns.hpp>
#include <tenzir/async/stream_from.hpp>
#include <tenzir/async/tcp.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/socket.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <cstring>
#include <memory>

namespace tenzir::plugins::from_tcp {

namespace {

constexpr auto connect_timeout = std::chrono::seconds{5};

auto socket_address_to_ip(folly::SocketAddress const& address) -> ip {
  auto storage = sockaddr_storage{};
  auto length = address.getAddress(&storage);
  TENZIR_ASSERT(length > 0);
  auto result = ip{};
  if (storage.ss_family == AF_INET) {
    auto sockaddr = sockaddr_in{};
    std::memcpy(&sockaddr, &storage, sizeof(sockaddr));
    auto err = convert(sockaddr, result);
    TENZIR_ASSERT(not err);
  } else {
    TENZIR_ASSERT(storage.ss_family == AF_INET6);
    auto sockaddr = sockaddr_in6{};
    std::memcpy(&sockaddr, &storage, sizeof(sockaddr));
    auto err = convert(sockaddr, result);
    TENZIR_ASSERT(not err);
  }
  return result;
}

struct TcpFrom {
  struct Args {
    located<std::string> endpoint;
    Option<located<data>> tls;
    located<ir::pipeline> user_pipeline;
    let_id peer_info;
  };

  struct ConnectionInfo {
    record peer_record;
    MetricsLabel bytes_metric_label;
  };

  explicit TcpFrom(Args args) : args_{std::move(args)} {
    auto ep = to<Endpoint>(args_.endpoint.inner);
    TENZIR_ASSERT(ep);
    TENZIR_ASSERT(ep->port);
    TENZIR_ASSERT(not ep->host.empty());
    host_ = ep->host;
    port_ = ep->port->number();
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
    auto resolved = co_await forward_dns_.resolve(host_);
    auto* addresses = resolved->is_err()
                        ? nullptr
                        : try_as<ForwardDnsResolved>(&resolved->unwrap());
    if (not addresses or addresses->answers.empty()) {
      auto diag = diagnostic::error("failed to resolve remote endpoint")
                    .primary(args_.endpoint.source)
                    .note("host: {}", host_);
      if (resolved->is_err()) {
        std::move(diag)
          .note("reason: {}", resolved->unwrap_err().error)
          .emit(ctx);
      } else {
        std::move(diag).note("reason: no matching A or AAAA records").emit(ctx);
      }
      co_return false;
    }
    auto address = folly::SocketAddress{};
    address.setFromIpPort(fmt::to_string(addresses->answers.front().address),
                          port_);
    address_ = std::move(address);
    co_return true;
  }

  auto connect(folly::EventBase* evb) const
    -> Task<Box<folly::coro::Transport>> {
    TENZIR_ASSERT(address_);
    TENZIR_DEBUG("connecting to {}", address_->describe());
    auto transport = co_await connect_tcp_client(
      evb, *address_,
      std::chrono::duration_cast<std::chrono::milliseconds>(connect_timeout),
      tls_context_, host_);
    TENZIR_DEBUG("connected to {}", address_->describe());
    co_return Box<folly::coro::Transport>{std::move(transport)};
  }

  auto make_connection_info(folly::coro::Transport const& transport,
                            OpCtx&) const -> ConnectionInfo {
    auto peer_addr = transport.getPeerAddress();
    auto peer_ip = socket_address_to_ip(peer_addr);
    return {
      .peer_record = record{
        {"ip", peer_ip},
        {"port", int64_t{peer_addr.getPort()}},
      },
      .bytes_metric_label = MetricsLabel{
        "peer_ip",
        MetricsLabel::FixedString::truncate(fmt::to_string(peer_ip)),
      },
    };
  }

  auto substitute(ir::pipeline& pipeline, ConnectionInfo& info,
                  OpCtx& ctx) const -> bool {
    auto env = substitute_ctx::env_t{};
    env[args_.peer_info] = std::move(info.peer_record);
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    return static_cast<bool>(
      pipeline.substitute(substitute_ctx{b_ctx, &env}, true));
  }

  auto pipeline() -> located<ir::pipeline>& {
    return args_.user_pipeline;
  }

  auto events_metric_label() const -> MetricsLabel {
    return {"operator", "from_tcp"};
  }

  auto bytes_metric_label(ConnectionInfo const& info) const -> MetricsLabel {
    return info.bytes_metric_label;
  }

  auto ready() const -> bool {
    return static_cast<bool>(address_);
  }

  auto emit_connect_warning(folly::AsyncSocketException const& ex,
                            diagnostic_handler& dh) const -> void {
    TENZIR_ASSERT(address_);
    auto diag
      = diagnostic::warning("failed to connect to {}", address_->describe())
          .primary(args_.endpoint.source)
          .note("reason: {}", ex.what())
          .hint("ensure a TCP server is listening on this endpoint");
    add_tls_client_diagnostic_hints(std::move(diag), tls_context_ != nullptr)
      .emit(dh);
  }

  auto emit_read_warning(std::string const& error, diagnostic_handler& dh) const
    -> void {
    TENZIR_ASSERT(address_);
    diagnostic::warning("connection closed after read error")
      .primary(args_.endpoint.source)
      .note("endpoint: {}", address_->describe())
      .note("reason: {}", error)
      .emit(dh);
  }

private:
  Args args_;
  Option<folly::SocketAddress> address_;
  std::string host_;
  uint16_t port_ = 0;
  ForwardDnsResolver forward_dns_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
};

using FromTcpArgs = TcpFrom::Args;
using FromTcp = StreamFrom<TcpFrom>;

class from_tcp_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromTcpArgs, FromTcp>{};
    auto endpoint_arg = d.positional("endpoint", &FromTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &FromTcpArgs::tls);
    auto pipeline_arg
      = d.pipeline(&FromTcpArgs::user_pipeline, SubOptimize::from_downstream,
                   {{"peer", &FromTcpArgs::peer_info}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto ep_str, ctx.get(endpoint_arg));
      auto ep = to<Endpoint>(ep_str.inner);
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
      TRY(auto pipeline, ctx.get(pipeline_arg));
      auto output = pipeline.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(pipeline.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_tcp::from_tcp_plugin)
