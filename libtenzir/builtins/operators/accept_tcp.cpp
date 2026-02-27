//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/tls.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/ViaIfAsync.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <unordered_map>

namespace tenzir::plugins::accept_tcp {

namespace {

constexpr auto read_timeout = std::chrono::seconds{1};
constexpr auto buffer_size = size_t{65536};
constexpr auto listen_backlog = uint32_t{128};
constexpr auto message_queue_capacity = uint32_t{1024};

struct AcceptTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  std::optional<located<uint64_t>> max_connections;
  located<ir::pipeline> user_pipeline;
  let_id peer_info;
};

class AcceptTcpListener final : public Operator<void, table_slice> {
public:
  struct Accepted {
    Box<folly::coro::Transport> transport;
  };
  struct ReadChunk {
    uint64_t conn_id;
    chunk_ptr chunk;
  };
  struct ConnectionClosed {
    uint64_t conn_id;
  };
  struct ConnectionError {
    uint64_t conn_id;
    std::string error;
  };
  using Message
    = variant<Accepted, ReadChunk, ConnectionClosed, ConnectionError>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  AcceptTcpListener(AcceptTcpArgs args)
    : endpoint_source_{args.endpoint.source},
      user_pipeline_{std::move(args.user_pipeline)},
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
    if (args.max_connections) {
      max_connections_ = args.max_connections->inner;
    }
  }

  AcceptTcpListener(const AcceptTcpListener&) = delete;
  AcceptTcpListener& operator=(const AcceptTcpListener&) = delete;
  AcceptTcpListener(AcceptTcpListener&&) noexcept = default;
  AcceptTcpListener& operator=(AcceptTcpListener&&) noexcept = default;
  ~AcceptTcpListener() override = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    if (tls_ and tls_->get_tls(nullptr).inner) {
      auto context = tls_->make_folly_ssl_context(ctx);
      if (not context) {
        done_ = true;
        co_return;
      }
      tls_context_ = std::move(*context);
    }
    TENZIR_DEBUG("starting listener on {}", address_.describe());
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    // Let ServerSocket handle bind/listen setup
    server_ = std::make_unique<folly::coro::ServerSocket>(
      std::move(socket), address_, listen_backlog);
    ctx.spawn_task(accept_loop(ctx.dh()));
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_return {};
    }
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    if (done_) {
      co_return;
    }
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](Accepted accepted) -> Task<void> {
        auto transport = std::move(accepted.transport);
        auto* transport_evb = transport->getEventBase();
        TENZIR_ASSERT(transport_evb);
        auto peer_addr = transport->getPeerAddress();
        if (connections_.size() >= max_connections_) {
          diagnostic::warning(
            "connection rejected: maximum number of connections reached")
            .primary(endpoint_source_)
            .note("peer: {}", peer_addr.describe())
            .note("max_connections: {}", max_connections_)
            .hint("increase `max_connections` if this level of concurrency is "
                  "expected")
            .emit(ctx);
          co_return;
        }
        if (tls_context_) {
          try {
            co_await upgrade_transport_to_tls_server(transport, tls_context_);
          } catch (folly::AsyncSocketException const& ex) {
            // Peer-driven TLS failures are expected at runtime; keep serving
            // other connections instead of failing the whole operator.
            diagnostic::warning("TLS handshake failed")
              .primary(endpoint_source_)
              .note("TLS handshake with peer {} failed", peer_addr.describe())
              .note("reason: {}", ex.what())
              .hint("verify TLS settings and certificates on both sides")
              .emit(ctx);
            co_return;
          }
        }
        auto peer_record = record{
          {"ip", peer_addr.getAddressStr()},
          {"port", int64_t{peer_addr.getPort()}},
        };
        auto conn_id = next_conn_id_++;
        auto pipeline_copy = user_pipeline_.inner;
        auto env = substitute_ctx::env_t{};
        env[peer_let_id_] = std::move(peer_record);
        auto bytes_read = ctx.make_counter(
          MetricsLabel{"peer_ip", MetricsLabel::FixedString::truncate(
                                    peer_addr.getAddressStr())},
          MetricsDirection::read, MetricsVisibility::external_);
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
        connections_.emplace(conn_id, std::move(open_pipeline));
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb, read_loop(conn_id, std::move(transport),
                                   *message_queue_, std::move(bytes_read))));
      },
      [&](ReadChunk read_chunk) -> Task<void> {
        if (auto it = connections_.find(read_chunk.conn_id);
            it != connections_.end()) {
          if ((co_await it->second.push(std::move(read_chunk.chunk))).is_err()) {
            connections_.erase(it);
          }
        }
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (auto it = connections_.find(closed.conn_id);
            it != connections_.end()) {
          co_await it->second.close();
          connections_.erase(it);
        }
      },
      [&](ConnectionError error) -> Task<void> {
        diagnostic::warning("connection closed after read error")
          .primary(endpoint_source_)
          .note("connection id: {}", error.conn_id)
          .note("reason: {}", error.error)
          .emit(ctx);
        if (auto it = connections_.find(error.conn_id);
            it != connections_.end()) {
          co_await it->second.close();
          connections_.erase(it);
        }
      });
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  auto accept_loop(diagnostic_handler& dh) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_ASSERT(evb_);
    while (true) {
      std::unique_ptr<folly::coro::Transport> transport;
      try {
        co_await folly::coro::co_safe_point;
        // `await_task` is now queue-based, so accepting runs in this task.
        transport
          = co_await folly::coro::co_withExecutor(evb_, server_->accept());
      } catch (folly::OperationCancelled const&) {
        // Cancellation is part of normal shutdown.
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        // Accept can fail transiently (e.g., client abort/reset). Keep the
        // listener alive and continue with the next accept attempt.
        diagnostic::warning("failed to accept incoming connection")
          .primary(endpoint_source_)
          .note("endpoint: {}", address_.describe())
          .note("reason: {}", ex.what())
          .emit(dh);
        continue;
      }
      TENZIR_DEBUG("accepted connection from {}",
                   transport->getPeerAddress().describe());
      co_await message_queue_->enqueue(Accepted{
        Box<folly::coro::Transport>::from_unique_ptr(std::move(transport))});
    }
  }

  static auto read_loop(uint64_t conn_id, Box<folly::coro::Transport> transport,
                        MessageQueue& message_queue,
                        MetricsCounter bytes_counter) -> Task<void> {
    while (true) {
      folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
      size_t bytes = 0;
      auto read_error = std::string{};
      try {
        bytes = co_await transport->read(
          buf, 1, buffer_size,
          std::chrono::duration_cast<std::chrono::milliseconds>(read_timeout));
      } catch (folly::OperationCancelled const&) {
        // Cancellation is part of normal shutdown.
        co_return;
      } catch (const folly::AsyncSocketException& e) {
        // Read timeouts are expected; other socket errors are connection-local.
        if (e.getType() == folly::AsyncSocketException::TIMED_OUT) {
          continue;
        }
        read_error = e.what();
      }
      if (not read_error.empty()) {
        co_await message_queue.enqueue(ConnectionError{conn_id, read_error});
        co_await message_queue.enqueue(ConnectionClosed{conn_id});
        co_return;
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
      co_await message_queue.enqueue(ReadChunk{conn_id, std::move(chk)});
    }
    co_await message_queue.enqueue(ConnectionClosed{conn_id});
  }

  location endpoint_source_ = location::unknown;
  folly::SocketAddress address_;
  located<ir::pipeline> user_pipeline_;
  let_id peer_let_id_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  std::unique_ptr<folly::coro::ServerSocket> server_;
  mutable Box<MessageQueue> message_queue_{
    std::in_place,
    message_queue_capacity,
  };
  std::unordered_map<uint64_t, OpenPipeline<chunk_ptr>> connections_;
  uint64_t max_connections_ = 128;
  bool done_ = false;
  mutable uint64_t next_conn_id_{0};
};

class AcceptTcpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.accept_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptTcpArgs, AcceptTcpListener>{};
    auto endpoint_arg = d.positional("endpoint", &AcceptTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &AcceptTcpArgs::tls);
    auto max_connections_arg
      = d.named("max_connections", &AcceptTcpArgs::max_connections);
    d.pipeline(&AcceptTcpArgs::user_pipeline,
               {{"peer", &AcceptTcpArgs::peer_info}});
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
      if (auto max_connections = ctx.get(max_connections_arg);
          max_connections and max_connections->inner == 0) {
        auto loc
          = ctx.get_location(max_connections_arg).value_or(location::unknown);
        diagnostic::error("max_connections must be greater than 0")
          .primary(loc)
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
