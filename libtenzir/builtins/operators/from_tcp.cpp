//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
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
    // Let ServerSocket handle bind/listen/accept setup
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, kListenBacklog);
    co_return;
  }

  auto await_task() const -> Task<std::any> override {
    TENZIR_VERBOSE("from_tcp: waiting for connection");
    auto transport = co_await server_->accept();
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
    io_scope_->add(folly::coro::co_withExecutor(
      folly::getGlobalIOExecutor(),
      read_loop(conn_id, transport_ptr, open_pipeline, ctx.dh())));
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_VERBOSE("from_tcp: finalizing, closing server");
    // Stop accepting new connections
    if (server_) {
      server_->close();
    }
    // Signal all read loops to stop
    io_scope_->requestCancellation();
    // Wait for all read loops to exit (they close their own transports)
    co_await io_scope_->joinAsync();
    connections_.clear();
    TENZIR_VERBOSE("from_tcp: finalize complete");
  }

private:
  static auto
  read_loop(uint64_t conn_id, std::shared_ptr<folly::coro::Transport> transport,
            OpenPipeline<chunk_ptr> pipeline, diagnostic_handler& dh)
    -> Task<void> {
    auto io_executor = folly::getGlobalIOExecutor();
    TENZIR_DEBUG("from_tcp[{}]: starting read loop", conn_id);
    try {
      while (true) {
        // Check for cancellation
        auto token = co_await folly::coro::co_current_cancellation_token;
        if (token.isCancellationRequested()) {
          TENZIR_DEBUG("from_tcp[{}]: cancellation requested", conn_id);
          break;
        }
        // Read with timeout - allows periodic cancellation checks
        folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
        size_t bytes = 0;
        try {
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
          TENZIR_WARN("from_tcp[{}]: socket error: {}", conn_id, e.what());
          break;
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
        // Push data to subpipeline
        // IMPORTANT: co_viaIfAsync ensures we return to IO executor after push
        (co_await folly::coro::co_viaIfAsync(io_executor,
                                             pipeline.push(std::move(chk))))
          .expect("pipeline closed unexpectedly");
      }
    } catch (const folly::OperationCancelled&) {
      TENZIR_DEBUG("from_tcp[{}]: operation cancelled", conn_id);
    } catch (const std::exception& e) {
      TENZIR_WARN("from_tcp[{}]: unexpected error: {}", conn_id, e.what());
    }
    // Signal subpipeline to finalize
    co_await folly::coro::co_viaIfAsync(io_executor, pipeline.close());
    // Close transport on correct thread (we're on IO executor)
    TENZIR_VERBOSE("from_tcp[{}]: closing transport", conn_id);
    transport->close();
  }

  folly::SocketAddress address_;
  ir::pipeline user_pipeline_;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  mutable std::vector<Connection> connections_;
  mutable std::unique_ptr<folly::coro::CancellableAsyncScope> io_scope_
    = std::make_unique<folly::coro::CancellableAsyncScope>();
  mutable uint64_t next_conn_id_{0};
};

class from_tcp_ir final : public ir::Operator {
public:
  from_tcp_ir() = default;

  from_tcp_ir(std::string endpoint, ir::pipeline pipe)
    : endpoint_{std::move(endpoint)}, pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "from_tcp";
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<void>());
    folly::SocketAddress address;
    // Parse endpoint - expected format: "host:port" or ":port"
    auto colon_pos = endpoint_.rfind(':');
    if (colon_pos == std::string::npos) {
      // Just a port number
      address.setFromLocalPort(std::stoi(endpoint_));
    } else {
      auto host = endpoint_.substr(0, colon_pos);
      auto port = std::stoi(endpoint_.substr(colon_pos + 1));
      if (host.empty()) {
        address.setFromLocalPort(port);
      } else {
        address.setFromHostPort(host, port);
      }
    }
    return FromTcpListener{std::move(address), std::move(pipe_)};
  }

  auto substitute(substitute_ctx /*ctx*/, bool /*instantiate*/)
    -> failure_or<void> override {
    // Don't substitute the inner pipeline here - it will be substituted
    // per-connection when spawning subpipelines
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (not input.is<void>()) {
      diagnostic::error("from_tcp is a source operator").emit(dh);
      return failure::promise();
    }
    // The output type depends on the user's pipeline
    // The subpipeline receives chunk_ptr and should output table_slice
    TRY(auto output, pipe_.infer_type(tag_v<chunk_ptr>, dh));
    if (not output) {
      return tag_v<table_slice>;
    }
    return output;
  }

  auto main_location() const -> location override {
    return location_;
  }

  friend auto inspect(auto& f, from_tcp_ir& x) -> bool {
    return f.object(x).fields(f.field("endpoint", x.endpoint_),
                              f.field("pipe", x.pipe_),
                              f.field("location", x.location_));
  }

private:
  std::string endpoint_;
  ir::pipeline pipe_;
  location location_ = location::unknown;
};

class from_tcp_plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_tcp";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
    // Expect exactly two arguments: endpoint and pipeline
    if (inv.args.size() != 2) {
      diagnostic::error("expected exactly two arguments: endpoint and pipeline")
        .primary(inv.op)
        .emit(ctx);
      return failure::promise();
    }

    // First argument: endpoint (should be a string)
    TRY(inv.args[0].bind(ctx));
    TRY(auto endpoint_data, const_eval(inv.args[0], ctx));
    auto* endpoint_str = try_as<std::string>(endpoint_data);
    if (not endpoint_str) {
      diagnostic::error("endpoint must be a string")
        .primary(inv.args[0])
        .emit(ctx);
      return failure::promise();
    }

    // Second argument: pipeline
    auto pipe = as<ast::pipeline_expr>(inv.args[1]);
    TRY(auto pipe_ir, std::move(pipe.inner).compile(ctx));

    return from_tcp_ir{*endpoint_str, std::move(pipe_ir)};
  }
};

} // namespace

} // namespace tenzir::plugins::from_tcp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_tcp::from_tcp_plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::from_tcp::from_tcp_ir>)
