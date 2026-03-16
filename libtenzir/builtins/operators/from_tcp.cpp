//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/tcp.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Sleep.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/Transport.h>

#include <memory>

namespace tenzir::plugins::from_tcp {

namespace {

using namespace tenzir::si_literals;

// Read at most 64 KiB per socket callback. This keeps the per-connection
// working set modest while still amortizing callback overhead well for TCP
// streams, and it aligns with the movable-buffer read path below where folly
// hands us owned buffers that we transfer directly into chunks.
constexpr auto buffer_size = size_t{64_Ki};
// Give remote endpoints long enough for a normal TCP plus TLS setup while
// still surfacing unavailable servers quickly to the retry loop.
constexpr auto connect_timeout = std::chrono::seconds{5};
// Reconnect almost immediately after the first failure so short races during
// fixture startup or service restarts do not stall the pipeline.
constexpr auto connect_initial_backoff = std::chrono::milliseconds{100};
// Cap retries at five seconds to avoid hammering broken endpoints while still
// keeping recovery reasonably quick once the peer comes back.
constexpr auto connect_max_backoff = std::chrono::milliseconds{5_k};
// Leave room for many in-flight read and close notifications without turning
// the queue into another large per-connection memory sink.
constexpr auto message_queue_capacity = uint32_t{1_Ki};

struct FromTcpArgs {
  located<std::string> endpoint;
  std::optional<located<data>> tls;
  located<ir::pipeline> user_pipeline;
  let_id peer_info;
};

class FromTcpConnector final : public Operator<void, table_slice> {
public:
  using Connection = Arc<folly::coro::Transport>;

  struct Connected {
    Box<folly::coro::Transport> transport;
  };

  struct Payload {
    uint64_t conn_id;
    chunk_ptr chunk;
  };

  struct ConnectionClosed {
    uint64_t conn_id;
    Option<std::string> error;
  };

  using Message = variant<Connected, Payload, ConnectionClosed>;
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
    if (current_connection_) {
      co_return co_await message_queue_->dequeue();
    }
    while (not done_) {
      TENZIR_DEBUG("connecting to {}", address_.describe());
      auto connect_error = Option<std::string>{};
      try {
        auto transport
          = Box<folly::coro::Transport>{co_await connect_tcp_client(
            evb_, address_,
            std::chrono::duration_cast<std::chrono::milliseconds>(
              connect_timeout),
            tls_context_, host_)};
        TENZIR_DEBUG("connected to {}", address_.describe());
        co_return Message{Connected{std::move(transport)}};
      } catch (folly::AsyncSocketException const& ex) {
        // Connection attempts can fail transiently; treat this as a retriable
        // network condition and keep the operator alive.
        connect_error = ex.what();
      }
      if (connect_error) {
        if (done_) {
          co_return {};
        }
        // TODO: Surface connect retries and failures as metrics in a
        // follow-up that covers all TCP operators.
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
          close_transport(std::move(transport));
          co_return;
        }
        auto sub = co_await ctx.spawn_sub(
          data{int64_t(conn_id)}, std::move(pipeline_copy), tag_v<chunk_ptr>);
        pipeline_ = as<OpenPipeline<chunk_ptr>>(sub);
        current_conn_id_ = conn_id;
        current_connection_ = Connection{std::move(*transport)};
        auto message_queue = message_queue_;
        ctx.spawn_task(folly::coro::co_withExecutor(
          transport_evb,
          read_loop(conn_id, *current_connection_, std::move(message_queue))));
      },
      [&](Payload payload) -> Task<void> {
        if (not pipeline_ or not current_conn_id_
            or payload.conn_id != *current_conn_id_) {
          co_return;
        }
        auto push_result = co_await pipeline_->push(std::move(payload.chunk));
        TENZIR_UNUSED(push_result);
      },
      [&](ConnectionClosed closed) -> Task<void> {
        if (closed.error) {
          // TODO: Surface routine TCP read failures and disconnects as metrics
          // in a follow-up that covers all TCP operators.
          diagnostic::warning("connection closed after read error")
            .primary(endpoint_source_)
            .note("endpoint: {}", address_.describe())
            .note("reason: {}", *closed.error)
            .emit(ctx);
        }
        if (current_conn_id_ and *current_conn_id_ == closed.conn_id) {
          current_connection_ = None{};
          current_conn_id_ = None{};
          if (pipeline_) {
            co_await pipeline_->close();
            pipeline_ = None{};
          }
        }
      });
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto conn_id = static_cast<uint64_t>(as<int64_t>(key));
    if (current_conn_id_ and *current_conn_id_ == conn_id) {
      pipeline_ = None{};
      if (current_connection_) {
        auto connection = std::move(*current_connection_);
        current_connection_ = None{};
        close_transport(std::move(connection));
      }
      current_conn_id_ = None{};
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  static auto close_transport(Connection connection) -> void {
    auto* evb = connection->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([connection = std::move(connection)]() mutable {
      connection->close();
    });
  }

  static auto close_transport(Box<folly::coro::Transport> transport) -> void {
    auto* evb = transport->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([transport = std::move(transport)]() mutable {
      transport->close();
    });
  }

  static auto read_loop(uint64_t conn_id, Connection connection,
                        Arc<MessageQueue> message_queue) -> Task<void> {
    auto read_error = std::string{};
    auto cancellation_token
      = co_await folly::coro::co_current_cancellation_token;
    while (true) {
      try {
        auto read_result = co_await folly::coro::co_withCancellation(
          cancellation_token, read_tcp_chunk(*connection, buffer_size,
                                             std::chrono::milliseconds{0}));
        if (not read_result) {
          break;
        }
        co_await message_queue->enqueue(
          Payload{conn_id, std::move(*read_result)});
      } catch (const folly::AsyncSocketException& e) {
        // Socket errors are connection-scoped; log and reconnect.
        read_error = e.what();
        break;
      }
    }
    co_await message_queue->enqueue(ConnectionClosed{
      conn_id,
      read_error.empty() ? Option<std::string>{}
                         : Option<std::string>{std::move(read_error)},
    });
  }

  location endpoint_source_ = location::unknown;
  folly::SocketAddress address_;
  std::string host_;
  located<ir::pipeline> user_pipeline_;
  let_id peer_let_id_;
  Option<tls_options> tls_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  folly::EventBase* evb_ = nullptr;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<OpenPipeline<chunk_ptr>> pipeline_;
  Option<Connection> current_connection_;
  Option<uint64_t> current_conn_id_;
  bool done_ = false;
  mutable std::chrono::milliseconds reconnect_backoff_{connect_initial_backoff};
  uint64_t next_conn_id_{0};
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
