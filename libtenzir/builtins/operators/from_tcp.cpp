//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/tls.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/ViaIfAsync.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

namespace tenzir::plugins::from_tcp {

namespace {

constexpr auto kReadTimeout = std::chrono::seconds{1};
constexpr auto kBufferSize = size_t{65536};
constexpr auto kListenBacklog = uint32_t{128};

struct FromTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  located<ir::pipeline> user_pipeline;
  let_id peer_info;
};

class FromTcpListener final : public Operator<void, table_slice> {
public:
  struct Connection {
    std::shared_ptr<folly::coro::Transport> transport;
  };

  FromTcpListener(FromTcpArgs args)
    : user_pipeline_{std::move(args.user_pipeline)},
      peer_let_id_{args.peer_info} {
    // Parse endpoint string to SocketAddress (validation already done in
    // describe)
    auto ep = to<struct endpoint>(args.endpoint.inner);
    TENZIR_ASSERT(ep);
    TENZIR_ASSERT(ep->port);
    if (ep->host.empty()) {
      address_.setFromLocalPort(ep->port->number());
    } else {
      address_.setFromHostPort(ep->host, ep->port->number());
    }
    if (args.tls) {
      tls_ = tls_options{*args.tls, {.is_server = true}};
    }
  }

  FromTcpListener(const FromTcpListener&) = delete;
  FromTcpListener& operator=(const FromTcpListener&) = delete;
  FromTcpListener(FromTcpListener&&) noexcept = default;
  FromTcpListener& operator=(FromTcpListener&&) noexcept = default;
  ~FromTcpListener() override = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    if (tls_ and tls_->get_tls(nullptr).inner) {
      auto context = tls_->make_folly_ssl_context(ctx);
      if (not context) {
        done_ = true;
        co_return;
      }
      tls_context_ = std::move(*context);
    }
    TENZIR_VERBOSE("from_tcp: starting listener on {}", address_.describe());
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    // Let ServerSocket handle bind/listen setup
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, kListenBacklog);
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    if (done_) {
      co_return {};
    }
    TENZIR_ASSERT(evb_);
    TENZIR_ASSERT(server_);
    TENZIR_VERBOSE("from_tcp: waiting for connection");
    auto transport
      = co_await folly::coro::co_withExecutor(evb_, server_->accept());
    TENZIR_INFO("from_tcp: accepted connection from {}",
                transport->getPeerAddress().describe());
    co_return Box<folly::coro::Transport>::from_non_null(std::move(transport));
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_) {
      co_return;
    }
    auto transport = std::move(result).as<Box<folly::coro::Transport>>();
    auto* transport_evb = transport->getEventBase();
    TENZIR_ASSERT(transport_evb);
    auto peer_addr = transport->getPeerAddress();
    if (tls_context_) {
      try {
        co_await upgrade_transport_to_tls_server(transport, tls_context_);
      } catch (std::exception const& ex) {
        diagnostic::warning("TLS handshake failed with {}: {}",
                            peer_addr.describe(), ex.what())
          .emit(ctx);
        co_return;
      }
    }
    // Create peer info record from the connection
    auto peer_record = record{
      {"ip", peer_addr.getAddressStr()},
      {"port", int64_t{peer_addr.getPort()}},
    };
    // Spawn subpipeline for this connection
    auto conn_id = next_conn_id_++;
    auto pipeline_copy = user_pipeline_.inner;
    // Substitute the pipeline copy with peer info in the environment
    auto env = substitute_ctx::env_t{};
    env[peer_let_id_] = std::move(peer_record);
    auto bytes_read = ctx.make_counter(
      MetricsLabel{"peer_ip", MetricsLabel::FixedString::truncate(
                                peer_addr.getAddressStr())},
      MetricsDirection::read, MetricsVisibility::external_);
    TENZIR_DEBUG("from_tcp: using peer_let_id_ = {}", peer_let_id_.id);
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    auto sub_result
      = pipeline_copy.substitute(substitute_ctx{b_ctx, &env}, true);
    if (not sub_result) {
      co_return;
    }
    auto sub = co_await ctx.spawn_sub(
      data{int64_t(conn_id)}, std::move(pipeline_copy), tag_v<chunk_ptr>);
    auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
    // Spawn read loop on IO executor
    ctx.spawn_task(folly::coro::co_withExecutor(
      transport_evb, read_loop(std::move(transport), open_pipeline, ctx.dh(),
                               std::move(bytes_read))));
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  static auto read_loop(Box<folly::coro::Transport> transport,
                        OpenPipeline<chunk_ptr> pipeline,
                        diagnostic_handler& dh, MetricsCounter bytes_counter)
    -> Task<void> {
    while (true) {
      folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
      size_t bytes = 0;
      try {
        bytes = co_await transport->read(
          buf, 1, kBufferSize,
          std::chrono::duration_cast<std::chrono::milliseconds>(kReadTimeout));
      } catch (const folly::AsyncSocketException& e) {
        if (e.getType() != folly::AsyncSocketException::TIMED_OUT) {
          // Timeout is expected - continue to check cancellation
          diagnostic::warning("{}", e).emit(dh);
          co_return;
        }
      }
      if (bytes == 0) {
        break;
      }
      bytes_counter.add(bytes);
      // Convert IOBuf to chunk
      auto iobuf = buf.move();
      auto data = std::vector<std::byte>{};
      data.reserve(iobuf->computeChainDataLength());
      for (auto& range : *iobuf) {
        auto* begin = reinterpret_cast<const std::byte*>(range.data());
        data.insert(data.end(), begin, begin + range.size());
      }
      auto chk = chunk::make(std::move(data));
      if ((co_await pipeline.push(std::move(chk))).is_err()) {
        co_return;
      }
    }
    // Signal subpipeline to finalize
    co_await pipeline.close();
  }

  folly::SocketAddress address_;
  located<ir::pipeline> user_pipeline_;
  let_id peer_let_id_;
  std::optional<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  bool done_ = false;
  mutable uint64_t next_conn_id_{0};
};

class from_tcp_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromTcpArgs, FromTcpListener>{};
    auto endpoint_arg = d.positional("endpoint", &FromTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &FromTcpArgs::tls);
    d.pipeline(&FromTcpArgs::user_pipeline,
               {{"peer", &FromTcpArgs::peer_info}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto ep_str, ctx.get(endpoint_arg));
      auto ep = to<struct endpoint>(ep_str.inner);
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
      return {};
    });

    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_tcp::from_tcp_plugin)
