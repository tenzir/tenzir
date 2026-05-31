//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/dns.hpp>
#include <tenzir/async/metrics.hpp>
#include <tenzir/async/stream_accept.hpp>
#include <tenzir/async/tls.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
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

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/SocketAddress.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <array>
#include <cstring>
#include <limits>
#include <memory>

namespace tenzir::plugins::accept_tcp {

namespace {

constexpr auto listen_backlog = uint32_t{128};
constexpr auto tls_probe_timeout = std::chrono::seconds{5};

struct AcceptTcpArgs {
  located<std::string> endpoint;
  Option<located<data>> tls;
  Option<located<uint64_t>> max_connections;
  bool resolve_hostnames = false;
  bool auto_detect_tls = false;
  located<ir::pipeline> user_pipeline;
  let_id peer_info;
};

struct PeerInfo {
  ip address;
  int64_t port;
};

struct TcpConnectionMetrics {
  TcpConnectionMetrics(folly::coro::Transport const& transport,
                       metric_handler handler)
    : handle{transport.getPeerAddress().describe()},
      handler{std::move(handler)} {
    TENZIR_ASSERT(transport.getPeerAddress().isFamilyInet());
  }

  std::string handle;
  metric_handler handler;
  Atomic<uint64_t> reads = {};
  Atomic<uint64_t> bytes_read = {};
  Atomic<bool> closed = false;

  auto record_read(size_t bytes) -> void {
    reads.fetch_add(1, std::memory_order_relaxed);
    bytes_read.fetch_add(bytes, std::memory_order_relaxed);
  }

  auto emit() -> void {
    handler.emit({
      {"handle", handle},
      {"reads", reads.exchange(0, std::memory_order_relaxed)},
      {"writes", uint64_t{0}},
      {"bytes_read", bytes_read.exchange(0, std::memory_order_relaxed)},
      {"bytes_written", uint64_t{0}},
    });
  }

  auto close() -> void {
    if (closed.exchange(true, std::memory_order_relaxed)) {
      return;
    }
    emit();
  }

  auto is_closed() const -> bool {
    return closed.load(std::memory_order_relaxed);
  }
};

auto make_peer_info(folly::SocketAddress const& address) -> PeerInfo {
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
  return {
    .address = result,
    .port = int64_t{address.getPort()},
  };
}

auto tcp_metrics_type() -> type {
  return {
    "tenzir.metrics.tcp",
    record_type{
      {"handle", string_type{}},
      {"reads", uint64_type{}},
      {"writes", uint64_type{}},
      {"bytes_read", uint64_type{}},
      {"bytes_written", uint64_type{}},
    },
  };
}

auto emit_tcp_metrics(Arc<TcpConnectionMetrics> metrics) -> Task<void> {
  while (true) {
    co_await sleep_for(defaults::metrics_interval);
    if (metrics->is_closed()) {
      co_return;
    }
    metrics->emit();
  }
}

struct TcpAccept {
  using Args = AcceptTcpArgs;
  using Connection = Arc<folly::coro::Transport>;

  struct AcceptedInfo {
    Connection transport;
    ip peer_ip;
    int64_t peer_port;
    Arc<ReverseDnsResult> peer_reverse_dns;
  };

  struct ConnectionState {
    Arc<TcpConnectionMetrics> metrics;
  };

  explicit TcpAccept(Args args)
    : args_{std::move(args)},
      reverse_dns_{std::in_place,
                   ReverseDnsConfig{
                     .max_in_flight = detail::narrow<size_t>(
                       args_.max_connections ? args_.max_connections->inner
                                             : uint64_t{128}),
                   }} {
    auto ep = to<Endpoint>(args_.endpoint.inner);
    TENZIR_ASSERT(ep);
    TENZIR_ASSERT(ep->port);
    bind_host_ = ep->host;
    bind_port_ = ep->port->number();
    if (args_.tls) {
      tls_ = tls_options{*args_.tls, {.is_server = true}};
    }
  }

  auto max_connections() const -> uint64_t {
    return args_.max_connections ? args_.max_connections->inner : uint64_t{128};
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
    if (bind_host_.empty()) {
      address_.setFromLocalPort(bind_port_);
      co_return true;
    }
    auto resolved = co_await forward_dns_.resolve(bind_host_);
    auto* addresses = resolved->is_err()
                        ? nullptr
                        : try_as<ForwardDnsResolved>(&resolved->unwrap());
    if (not addresses or addresses->answers.empty()) {
      auto diag = diagnostic::error("failed to resolve listen address")
                    .primary(args_.endpoint.source)
                    .note("host: {}", bind_host_);
      if (resolved->is_err()) {
        std::move(diag)
          .note("reason: {}", resolved->unwrap_err().error)
          .emit(ctx);
      } else {
        std::move(diag).note("reason: no matching A or AAAA records").emit(ctx);
      }
      co_return false;
    }
    address_.setFromIpPort(fmt::to_string(addresses->answers.front().address),
                           bind_port_);
    co_return true;
  }

  auto start_listener(folly::EventBase* evb, OpCtx& ctx) -> Task<bool> {
    TENZIR_DEBUG("starting listener on {}", address_.describe());
    auto socket = folly::AsyncServerSocket::newSocket(evb);
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, listen_backlog);
    tcp_metrics_ = make_metric_handler(ctx, tcp_metrics_type());
    co_return true;
  }

  auto stop_accepting(folly::EventBase* evb) -> void {
    if (server_ and evb) {
      evb->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
        if (server_) {
          server_->close();
        }
      });
    }
  }

  auto cleanup() -> void {
  }

  auto accept(folly::EventBase* evb) -> Task<Box<folly::coro::Transport>> {
    TENZIR_ASSERT(server_);
    auto transport
      = co_await folly::coro::co_withExecutor(evb, server_->accept());
    co_return Box<folly::coro::Transport>::from_non_null(std::move(transport));
  }

  auto finish_accept(Box<folly::coro::Transport> transport,
                     folly::CancellationToken current_token,
                     folly::CancellationToken local_token,
                     folly::CancellationToken cancel_token,
                     diagnostic_handler& dh) -> Task<Option<AcceptedInfo>> {
    auto peer = transport->getPeerAddress();
    if (tls_context_) {
      try {
        auto should_upgrade = true;
        if (args_.auto_detect_tls) {
          auto* transport_evb = transport->getEventBase();
          TENZIR_ASSERT(transport_evb);
          auto probe = co_await folly::coro::co_withExecutor(
            transport_evb, probe_tls_client_hello(*transport));
          should_upgrade = probe.is_tls;
        }
        if (should_upgrade) {
          transport = Box<folly::coro::Transport>{
            co_await upgrade_transport_to_tls_server(std::move(*transport),
                                                     tls_context_)};
        }
      } catch (folly::AsyncSocketException const& ex) {
        diagnostic::warning("TLS handshake failed")
          .primary(args_.endpoint.source)
          .note("peer IP: {}", make_peer_info(peer).address)
          .note("reason: {}", ex.what())
          .hint("verify TLS settings and certificates on both sides")
          .emit(dh);
        co_return None{};
      }
    }
    auto peer_info = make_peer_info(peer);
    auto peer_reverse_dns
      = Arc<ReverseDnsResult>{ReverseDnsLookup{DnsNotFound{}}};
    if (args_.resolve_hostnames) {
      try {
        peer_reverse_dns = co_await folly::coro::co_withCancellation(
          cancel_token, reverse_dns_->resolve(peer_info.address));
      } catch (folly::OperationCancelled const&) {
        if (current_token.isCancellationRequested()
            and not local_token.isCancellationRequested()) {
          throw;
        }
        co_return None{};
      }
    }
    TENZIR_DEBUG("accepted connection from {}", peer.describe());
    co_return AcceptedInfo{
      .transport = Connection{std::move(*transport)},
      .peer_ip = peer_info.address,
      .peer_port = peer_info.port,
      .peer_reverse_dns = std::move(peer_reverse_dns),
    };
  }

  auto substitute(ir::pipeline& pipeline, AcceptedInfo& info, OpCtx& ctx)
    -> bool {
    auto peer_record = record{
      {"ip", info.peer_ip},
      {"port", info.peer_port},
    };
    if (args_.resolve_hostnames) {
      auto hostname = Option<std::string>{};
      if (not info.peer_reverse_dns->is_err()) {
        if (auto* resolved
            = try_as<ReverseDnsResolved>(&info.peer_reverse_dns->unwrap())) {
          hostname = resolved->hostname;
        }
      }
      peer_record.emplace("hostname", std::move(hostname));
      if (info.peer_reverse_dns->is_err()
          and not peer_resolution_warning_emitted_) {
        diagnostic::warning("{}", info.peer_reverse_dns->unwrap_err().error)
          .note("failed to resolve peer hostname for {}", info.peer_ip)
          .note("set `resolve_hostnames=false` to disable hostname resolution")
          .primary(args_.endpoint.source)
          .emit(ctx);
        peer_resolution_warning_emitted_ = true;
      }
    }
    auto env = substitute_ctx::env_t{};
    env[args_.peer_info] = std::move(peer_record);
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    return static_cast<bool>(
      pipeline.substitute(substitute_ctx{b_ctx, &env}, true));
  }

  auto make_connection_state(folly::coro::Transport& transport, AcceptedInfo&,
                             OpCtx& ctx) const -> ConnectionState {
    auto metrics = Arc<TcpConnectionMetrics>{
      std::in_place,
      transport,
      tcp_metrics_,
    };
    ctx.spawn_task(emit_tcp_metrics(metrics));
    return {.metrics = std::move(metrics)};
  }

  static auto record_read(ConnectionState& state, size_t bytes) -> void {
    state.metrics->record_read(bytes);
  }

  static auto close_connection_state(ConnectionState& state) -> void {
    state.metrics->close();
  }

  auto pipeline() -> located<ir::pipeline>& {
    return args_.user_pipeline;
  }

  auto events_metric_label() const -> MetricsLabel {
    return {"operator", "accept_tcp"};
  }

  auto bytes_metric_label(AcceptedInfo const& info) const -> MetricsLabel {
    return {
      "peer_ip",
      MetricsLabel::FixedString::truncate(fmt::to_string(info.peer_ip)),
    };
  }

  auto emit_accept_warning(folly::AsyncSocketException const& ex,
                           diagnostic_handler& dh) const -> void {
    diagnostic::warning("failed to accept incoming connection")
      .primary(args_.endpoint.source)
      .note("endpoint: {}", address_.describe())
      .note("reason: {}", ex.what())
      .emit(dh);
  }

  auto emit_read_warning(uint64_t conn_id, std::string const& error,
                         diagnostic_handler& dh) const -> void {
    diagnostic::warning("connection closed after read error")
      .primary(args_.endpoint.source)
      .note("connection id: {}", conn_id)
      .note("reason: {}", error)
      .emit(dh);
  }

  auto debug_name() const -> std::string_view {
    return "accept_tcp";
  }

private:
  struct TlsProbeResult {
    bool is_tls = false;
  };

  static auto could_be_tls_client_hello(std::span<std::byte const> bytes)
    -> bool {
    if (bytes.empty()) {
      return false;
    }
    if (bytes[0] != std::byte{0x16}) {
      return false;
    }
    if (bytes.size() >= 2 and bytes[1] != std::byte{0x03}) {
      return false;
    }
    if (bytes.size() >= 3 and bytes[2] > std::byte{0x04}) {
      return false;
    }
    if (bytes.size() >= 6 and bytes[5] != std::byte{0x01}) {
      return false;
    }
    return true;
  }

  static auto looks_like_tls_client_hello(std::span<std::byte const> bytes)
    -> bool {
    return bytes.size() >= 6 and could_be_tls_client_hello(bytes);
  }

  static auto set_pre_received_data(folly::coro::Transport& transport,
                                    std::span<std::byte const> bytes) -> void {
    auto* socket = dynamic_cast<folly::AsyncSocket*>(transport.getTransport());
    if (not socket) {
      throw folly::AsyncSocketException{
        folly::AsyncSocketException::INTERNAL_ERROR,
        "transport is not backed by AsyncSocket",
      };
    }
    socket->setPreReceivedData(
      folly::IOBuf::copyBuffer(bytes.data(), bytes.size()));
  }

  static auto probe_tls_client_hello(folly::coro::Transport& transport)
    -> Task<TlsProbeResult> {
    auto prefix = std::array<std::byte, 6>{};
    auto size = size_t{0};
    auto deadline = std::chrono::steady_clock::now() + tls_probe_timeout;
    auto throw_timed_out = [] {
      throw folly::AsyncSocketException{
        folly::AsyncSocketException::TIMED_OUT,
        "TLS auto-detect probe timed out",
      };
    };
    auto read_some = [&](size_t min_size) -> Task<void> {
      while (size < min_size) {
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
          throw_timed_out();
        }
        auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - now);
        if (timeout <= std::chrono::milliseconds{0}) {
          throw_timed_out();
        }
        auto* data = reinterpret_cast<unsigned char*>(prefix.data() + size);
        auto n
          = co_await transport.read(folly::MutableByteRange{data, 1}, timeout);
        if (n == 0) {
          co_return;
        }
        size += n;
        if (not could_be_tls_client_hello(std::span{prefix.data(), size})) {
          co_return;
        }
      }
    };
    auto timed_out = false;
    try {
      co_await read_some(1);
      if (size == 0) {
        co_return {};
      }
      if (prefix[0] == std::byte{0x16}) {
        co_await read_some(prefix.size());
      }
    } catch (folly::AsyncSocketException const& ex) {
      if (ex.getType() != folly::AsyncSocketException::TIMED_OUT) {
        throw;
      }
      timed_out = true;
    }
    auto bytes = std::span{prefix.data(), size};
    if (size > 0) {
      set_pre_received_data(transport, bytes);
    }
    if (looks_like_tls_client_hello(bytes)
        or (timed_out and could_be_tls_client_hello(bytes))) {
      co_return TlsProbeResult{.is_tls = true};
    }
    co_return {};
  }

  Args args_;
  folly::SocketAddress address_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  std::string bind_host_;
  uint16_t bind_port_ = 0;
  ForwardDnsResolver forward_dns_;
  Arc<ReverseDnsResolver> reverse_dns_;
  metric_handler tcp_metrics_ = {};
  bool peer_resolution_warning_emitted_ = false;
};

using AcceptTcp = StreamAccept<TcpAccept>;

class AcceptTcpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.accept_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptTcpArgs, AcceptTcp>{};
    auto endpoint_arg = d.positional("endpoint", &AcceptTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &AcceptTcpArgs::tls);
    auto max_connections_arg
      = d.named("max_connections", &AcceptTcpArgs::max_connections);
    d.named("resolve_hostnames", &AcceptTcpArgs::resolve_hostnames);
    auto auto_detect_tls_arg
      = d.named("auto_detect_tls", &AcceptTcpArgs::auto_detect_tls);
    auto pipeline_arg
      = d.pipeline(&AcceptTcpArgs::user_pipeline, SubOptimize::from_downstream,
                   {{"peer", &AcceptTcpArgs::peer_info}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto ep_str, ctx.get(endpoint_arg));
      auto ep = to<Endpoint>(ep_str.inner);
      auto loc = ctx.get_location(endpoint_arg).value_or(location::unknown);
      if (not ep) {
        diagnostic::error("failed to parse endpoint").primary(loc).emit(ctx);
      } else if (not ep->port) {
        diagnostic::error("port number is required").primary(loc).emit(ctx);
      }
      auto tls_enabled = false;
      if (auto tls_val = ctx.get(tls_arg)) {
        auto tls_opts = tls_options{*tls_val, {.is_server = true}};
        if (auto valid = tls_opts.validate(ctx); not valid) {
          return {};
        }
        tls_enabled = tls_opts.get_tls().inner;
      }
      if (auto max_connections = ctx.get(max_connections_arg);
          max_connections) {
        auto loc
          = ctx.get_location(max_connections_arg).value_or(location::unknown);
        if (max_connections->inner == 0) {
          diagnostic::error("max_connections must be greater than 0")
            .primary(loc)
            .emit(ctx);
        } else if (max_connections->inner > static_cast<uint64_t>(
                     std::numeric_limits<size_t>::max())) {
          diagnostic::error("max_connections is too large")
            .primary(loc)
            .note("maximum supported value: {}",
                  std::numeric_limits<size_t>::max())
            .emit(ctx);
        }
      }
      if (auto auto_detect_tls = ctx.get(auto_detect_tls_arg);
          auto_detect_tls and *auto_detect_tls and not tls_enabled) {
        diagnostic::error("`auto_detect_tls` requires TLS to be enabled")
          .primary(
            ctx.get_location(auto_detect_tls_arg).value_or(location::unknown))
          .emit(ctx);
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

} // namespace tenzir::plugins::accept_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_tcp::AcceptTcpPlugin)
