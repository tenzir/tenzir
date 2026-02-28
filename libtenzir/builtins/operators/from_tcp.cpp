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
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Sleep.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/Transport.h>

#include <algorithm>

namespace tenzir::plugins::from_tcp {

namespace {

constexpr auto read_timeout = std::chrono::seconds{1};
constexpr auto buffer_size = size_t{65536};
constexpr auto connect_timeout = std::chrono::seconds{5};
constexpr auto connect_initial_backoff = std::chrono::milliseconds{100};
constexpr auto connect_max_backoff = std::chrono::milliseconds{5000};
constexpr auto message_queue_capacity = uint32_t{1024};

struct FromTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  located<ir::pipeline> user_pipeline;
  let_id peer_info;
};

class FromTcpConnector final : public Operator<void, table_slice> {
public:
  struct Connected {
    Box<folly::coro::Transport> transport;
  };

  struct ReadChunk {
    chunk_ptr chunk;
  };

  struct ConnectionClosed {};

  struct ConnectionError {
    std::string error;
  };

  using Message
    = variant<Connected, ReadChunk, ConnectionClosed, ConnectionError>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  explicit FromTcpConnector(FromTcpArgs args)
    : endpoint_source_{args.endpoint.source},
      user_pipeline_{std::move(args.user_pipeline)},
      peer_let_id_{args.peer_info} {
    auto ep = to<struct endpoint>(args.endpoint.inner);
    TENZIR_ASSERT(ep);
    TENZIR_ASSERT(ep->port);
    TENZIR_ASSERT(not ep->host.empty());
    address_.setFromHostPort(ep->host, ep->port->number());
    host_ = ep->host;
    if (args.tls) {
      tls_ = tls_options{*args.tls, {.is_server = false}};
    }
  }

  FromTcpConnector(const FromTcpConnector&) = delete;
  FromTcpConnector& operator=(const FromTcpConnector&) = delete;
  FromTcpConnector(FromTcpConnector&&) noexcept = default;
  FromTcpConnector& operator=(FromTcpConnector&&) noexcept = default;
  ~FromTcpConnector() override = default;

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
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    if (done_) {
      co_return {};
    }
    if (connection_active_) {
      co_return co_await message_queue_->dequeue();
    }
    while (not done_) {
      TENZIR_DEBUG("connecting to {}", address_.describe());
      auto connect_error = Option<std::string>{};
      try {
        auto transport = co_await folly::coro::co_withExecutor(
          evb_, folly::coro::Transport::newConnectedSocket(evb_, address_,
                                                           connect_timeout));
        connection_active_ = true;
        TENZIR_DEBUG("connected to {}", address_.describe());
        co_return Message{
          Connected{Box<folly::coro::Transport>{std::move(transport)}}};
      } catch (const folly::OperationCancelled&) {
        // Cancellation is part of normal shutdown.
        co_return {};
      } catch (folly::AsyncSocketException const& ex) {
        // Connection attempts can fail transiently; treat this as a retriable
        // network condition and keep the operator alive.
        connect_error = ex.what();
      }
      if (connect_error) {
        if (done_) {
          co_return {};
        }
        diagnostic::warning("failed to connect to {}", address_.describe())
          .primary(endpoint_source_)
          .note("reason: {}", *connect_error)
          .hint("ensure a TCP server is listening on this endpoint")
          .emit(dh);
        auto backoff = reconnect_backoff_;
        reconnect_backoff_
          = std::min(reconnect_backoff_ * 2, connect_max_backoff);
        co_await folly::coro::sleep(backoff);
      }
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    if (done_) {
      co_return;
    }
    auto* message = result.try_as<Message>();
    if (not message) {
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](Connected connected) -> Task<void> {
        auto transport = std::move(connected.transport);
        auto* transport_evb = transport->getEventBase();
        TENZIR_ASSERT(transport_evb);
        auto tls_backoff = Option<std::chrono::milliseconds>{};
        if (tls_context_) {
          try {
            co_await upgrade_transport_to_tls_client(transport, tls_context_,
                                                     host_);
          } catch (folly::AsyncSocketException const& ex) {
            // TLS handshake failures are peer/network errors and should not
            // crash the operator; reconnect on the next await_task cycle.
            diagnostic::warning("TLS handshake with {} failed",
                                address_.describe())
              .primary(endpoint_source_)
              .note("reason: {}", ex.what())
              .hint("verify TLS settings and certificates on both sides")
              .emit(ctx);
            connection_active_ = false;
            tls_backoff = reconnect_backoff_;
            reconnect_backoff_
              = std::min(reconnect_backoff_ * 2, connect_max_backoff);
          }
        }
        if (tls_backoff) {
          try {
            co_await folly::coro::sleep(*tls_backoff);
          } catch (const folly::OperationCancelled&) {
            // Cancellation is part of normal shutdown.
            co_return;
          }
          co_return;
        }
        reconnect_backoff_ = connect_initial_backoff;
        auto peer_addr = transport->getPeerAddress();
        auto peer_record = record{
          {"ip", peer_addr.getAddressStr()},
          {"port", int64_t{peer_addr.getPort()}},
        };
        auto conn_id = next_conn_id_++;
        auto pipeline_copy = user_pipeline_.inner;
        auto env = substitute_ctx::env_t{};
        env[peer_let_id_] = std::move(peer_record);
        auto reg = global_registry();
        auto b_ctx = base_ctx{ctx, *reg};
        auto sub_result
          = pipeline_copy.substitute(substitute_ctx{b_ctx, &env}, true);
        if (not sub_result) {
          connection_active_ = false;
          co_return;
        }
        auto sub = co_await ctx.spawn_sub(
          data{int64_t(conn_id)}, std::move(pipeline_copy), tag_v<chunk_ptr>);
        pipeline_ = as<OpenPipeline<chunk_ptr>>(sub);
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb, read_loop(std::move(transport), *message_queue_)));
      },
      [&](ReadChunk read_chunk) -> Task<void> {
        if (not pipeline_) {
          co_return;
        }
        if ((co_await pipeline_->push(std::move(read_chunk.chunk))).is_err()) {
          co_await pipeline_->close();
          pipeline_ = None{};
          connection_active_ = false;
          done_ = true;
        }
      },
      [&](ConnectionClosed) -> Task<void> {
        if (pipeline_) {
          co_await pipeline_->close();
          pipeline_ = None{};
        }
        connection_active_ = false;
      },
      [&](ConnectionError error) -> Task<void> {
        diagnostic::warning("connection closed after read error")
          .primary(endpoint_source_)
          .note("endpoint: {}", address_.describe())
          .note("reason: {}", error.error)
          .emit(ctx);
        if (pipeline_) {
          co_await pipeline_->close();
          pipeline_ = None{};
        }
        connection_active_ = false;
      });
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (pipeline_) {
      co_await pipeline_->close();
      pipeline_ = None{};
    }
    connection_active_ = false;
    done_ = true;
    TENZIR_UNUSED(message_queue_->try_enqueue(ConnectionClosed{}));
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  static auto read_loop(Box<folly::coro::Transport> transport,
                        MessageQueue& message_queue) -> Task<void> {
    while (true) {
      folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
      size_t bytes = 0;
      auto read_error = std::string{};
      try {
        bytes = co_await transport->read(
          buf, 1, buffer_size,
          std::chrono::duration_cast<std::chrono::milliseconds>(read_timeout));
      } catch (const folly::OperationCancelled&) {
        // Cancellation is part of normal shutdown.
        co_return;
      } catch (const folly::AsyncSocketException& e) {
        // A read timeout is expected because we poll periodically for
        // cancellation and shutdown.
        if (e.getType() == folly::AsyncSocketException::TIMED_OUT) {
          continue;
        }
        // Other socket errors are connection-scoped; log and reconnect.
        read_error = e.what();
      }
      if (not read_error.empty()) {
        co_await message_queue.enqueue(ConnectionError{read_error});
        co_return;
      }
      if (bytes == 0) {
        break;
      }
      auto iobuf = buf.move();
      auto data = std::vector<std::byte>{};
      data.reserve(iobuf->computeChainDataLength());
      for (auto& range : *iobuf) {
        auto* begin = reinterpret_cast<const std::byte*>(range.data());
        data.insert(data.end(), begin, begin + range.size());
      }
      auto chk = chunk::make(std::move(data));
      co_await message_queue.enqueue(ReadChunk{std::move(chk)});
    }
    co_await message_queue.enqueue(ConnectionClosed{});
  }

  location endpoint_source_ = location::unknown;
  folly::SocketAddress address_;
  std::string host_;
  located<ir::pipeline> user_pipeline_;
  let_id peer_let_id_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  mutable Box<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<OpenPipeline<chunk_ptr>> pipeline_;
  bool done_ = false;
  mutable bool connection_active_ = false;
  mutable std::chrono::milliseconds reconnect_backoff_{connect_initial_backoff};
  mutable uint64_t next_conn_id_{0};
};

class from_tcp_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_tcp";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromTcpArgs, FromTcpConnector>{};
    auto endpoint_arg = d.positional("endpoint", &FromTcpArgs::endpoint);
    auto tls_arg = d.named("tls", &FromTcpArgs::tls);
    auto pipeline_arg = d.pipeline(&FromTcpArgs::user_pipeline,
                                   {{"peer", &FromTcpArgs::peer_info}});
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto ep_str, ctx.get(endpoint_arg));
      auto ep = to<struct endpoint>(ep_str.inner);
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
