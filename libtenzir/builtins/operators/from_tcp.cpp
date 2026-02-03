//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
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

class FromTcpListener final : public Operator<void, table_slice> {
public:
  struct Connection {
    std::shared_ptr<folly::coro::Transport> transport;
  };

  FromTcpListener(folly::SocketAddress address, ir::pipeline user_pipeline)
    : address_{std::move(address)}, user_pipeline_{std::move(user_pipeline)} {
  }

  FromTcpListener(FromTcpListener&&) = default;
  FromTcpListener& operator=(FromTcpListener&&)  noexcept = default;
  ~FromTcpListener() override = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    TENZIR_VERBOSE("from_tcp: starting listener on {}", address_.describe());
    auto* evb = folly::getGlobalIOExecutor()->getEventBase();
    auto socket = folly::AsyncServerSocket::newSocket(evb);
    // Let ServerSocket handle bind/listen setup
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, kListenBacklog);
    co_return;
  }

  auto await_task() const -> Task<std::any> override {
    TENZIR_VERBOSE("from_tcp: waiting for connection");
    auto transport = co_await folly::coro::co_withExecutor(
      folly::getGlobalIOExecutor(),
      server_->accept());
    TENZIR_INFO("from_tcp: accepted connection from {}",
                transport->getPeerAddress().describe());
    // Wrap in shared_ptr because std::any requires copyable types
    auto transport_ptr
      = std::make_shared<folly::coro::Transport>(std::move(*transport));
    co_return transport_ptr;
  }

  auto process_task(std::any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto transport_ptr = std::any_cast<std::shared_ptr<folly::coro::Transport>>(
      std::move(result));

    // Spawn subpipeline for this connection
    auto conn_id = next_conn_id_++;
    auto pipeline_copy = user_pipeline_;
    // Substitute the pipeline copy
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    auto sub_result
      = pipeline_copy.substitute(substitute_ctx{b_ctx, nullptr}, true);
    if (not sub_result) {
      diagnostic::error("failed to substitute pipeline for connection")
        .emit(ctx);
      co_return;
    }
    auto sub = co_await ctx.spawn_sub(
      data{int64_t(conn_id)}, std::move(pipeline_copy), tag_v<chunk_ptr>);
    auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);

    // Store connection for tracking
    connections_.push_back({transport_ptr});

    // Spawn read loop on IO executor
    ctx.spawn_task(folly::coro::co_withExecutor(
      folly::getGlobalIOExecutor(),
      read_loop(conn_id, transport_ptr, open_pipeline, ctx.dh())));
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_VERBOSE("from_tcp: finalizing, closing server");
    // Stop accepting new connections
    if (server_) {
      server_->close();
    }
  }

private:
  // TODO: Make OpenPipeline thread safe.
  static auto
  read_loop(uint64_t conn_id, std::shared_ptr<folly::coro::Transport> transport,
            OpenPipeline<chunk_ptr> pipeline, diagnostic_handler& dh)
    -> Task<void> {
    auto io_executor = folly::getGlobalIOExecutor();
    TENZIR_DEBUG("from_tcp[{}]: starting read loop", conn_id);
    while (true) {
      // Read with timeout - allows periodic cancellation checks
      folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
      size_t bytes = 0;
      try {
        // TODO: check if we really need the timeout.
        // TODO: check whether read support cancellation.
        bytes = co_await transport->read(
          buf, 1, kBufferSize,
          std::chrono::duration_cast<std::chrono::milliseconds>(
            kReadTimeout));
      } catch (const folly::AsyncSocketException& e) {
        if (e.getType() == folly::AsyncSocketException::TIMED_OUT) {
          // Timeout is expected - continue to check cancellation
          continue;
        }
        // TODO: demote or convert to diagnostic.
        diagnostic::warning("{}", e).emit(dh);
        co_return;
      }
      if (bytes == 0) {
        TENZIR_DEBUG("from_tcp[{}]: EOF, client closed connection",
                       conn_id);
        break;
      }
      TENZIR_DEBUG("from_tcp[{}]: read {} bytes", conn_id, bytes);
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
  ir::pipeline user_pipeline_;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  mutable std::vector<Connection> connections_;
  mutable uint64_t next_conn_id_{0};
};

struct FromTcpArgs {
  folly::SocketAddress address;
  ir::pipeline user_pipeline;
  let_id peer_info;
};

class from_tcp_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromTcpArgs, FromTcpListener>{};
    // CLAUDE: implement this API function in Describer.
    d.positional("endpoint", [](std::string endpoint, FromTcpArgs& args,
                                diagnostic_handler& dh) {
      auto ep = to<struct endpoint>(endpoint);
      // TODO: Add locations.
      // TODO: Move validation into validate? Probably not?
      if (not ep) {
        diagnostic::error("failed to parse endpoint").emit(dh);
        return failure::promise();
      }
      if (not ep->port) {
        diagnostic::error("port number is required").emit(dh);
        return failure::promise();
      }
      if (ep->host.empty()) {
        // Just a port number
        args.address.setFromLocalPort(ep->port->number());
      } else {
        args.address.setFromHostPort(ep->host, ep->port->number());
      }
    });
    // CLAUDE: end of call.
    d.pipeline(&FromTcpArgs::user_pipeline, {"peer", &FromTcpArgs::peer_info});
  }
};

} // namespace

} // namespace tenzir::plugins::from_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_tcp::from_tcp_plugin)
