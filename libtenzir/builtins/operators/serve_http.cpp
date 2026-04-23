//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_server.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/variant.hpp>

#include <folly/coro/BoundedQueue.h>
#include <folly/io/IOBuf.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <proxygen/lib/http/coro/HTTPStreamSource.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <proxygen/lib/http/coro/server/ScopedHTTPServer.h>

#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::serve_http {

namespace {

constexpr auto message_queue_capacity = uint32_t{512};
constexpr auto default_content_type
  = std::string_view{"application/octet-stream"};
constexpr auto egress_buffer_size = uint32_t{64 * 1024};

struct ServeHttpArgs {
  located<std::string> endpoint;
  Option<located<data>> tls;
  Option<located<uint64_t>> max_connections;
  located<ir::pipeline> printer;

  auto get_max_connections() const -> uint64_t {
    return max_connections ? max_connections->inner : uint64_t{128};
  }
};

auto make_response_headers(uint16_t status, std::string_view content_type)
  -> std::unique_ptr<proxygen::HTTPMessage> {
  auto message = std::make_unique<proxygen::HTTPMessage>();
  message->setStatusCode(status);
  message->setStatusMessage(proxygen::HTTPMessage::getDefaultReason(status));
  if (not content_type.empty()) {
    message->getHeaders().set(proxygen::HTTP_HEADER_CONTENT_TYPE,
                              std::string{content_type});
  }
  return message;
}

auto make_body_queue(chunk_ptr const& chunk) -> quic::BufQueue {
  TENZIR_ASSERT(chunk);
  auto buffer = folly::IOBuf::copyBuffer(
    reinterpret_cast<char const*>(chunk->data()), chunk->size());
  return quic::BufQueue{std::move(buffer)};
}

class Client final : public proxygen::coro::HTTPStreamSource::Callback,
                     public std::enable_shared_from_this<Client> {
public:
  static auto make(uint64_t id, folly::EventBase* evb, SemaphorePermit permit)
    -> std::shared_ptr<Client> {
    return std::shared_ptr<Client>{new Client{id, evb, std::move(permit)}};
  }

  ~Client() override {
    TENZIR_ASSERT(not source_.has_value());
  }

  auto start() -> void {
    lib_ref_ = shared_from_this();
  }

  auto source() -> proxygen::coro::HTTPStreamSource* {
    return source_ ? &*source_ : nullptr;
  }

  auto evb() const -> folly::EventBase* {
    return evb_;
  }

  auto closed() const -> bool {
    return closed_.load(std::memory_order_acquire);
  }

  void sourceComplete(proxygen::HTTPCodec::StreamID,
                      folly::Optional<proxygen::coro::HTTPError>) override {
    closed_.store(true, std::memory_order_release);
    permit_ = None{};
    source_.reset();
    lib_ref_.reset();
  }

  void windowOpen(proxygen::HTTPCodec::StreamID) override {
  }

  uint64_t id = 0;
  bool headers_sent = false;
  bool closing = false;

private:
  explicit Client(uint64_t id, folly::EventBase* evb, SemaphorePermit permit)
    : id{id},
      evb_{evb},
      source_{std::in_place, evb, folly::none, this, egress_buffer_size},
      permit_{std::move(permit)} {
  }

  folly::EventBase* evb_ = nullptr;
  std::optional<proxygen::coro::HTTPStreamSource> source_;
  Option<SemaphorePermit> permit_;
  std::shared_ptr<Client> lib_ref_;
  Atomic<bool> closed_ = false;
};

struct ClientStarted {
  std::shared_ptr<Client> client;
};

struct Payload {
  chunk_ptr chunk;
};

using Message = variant<ClientStarted, Payload>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

class RequestHandler final : public proxygen::coro::HTTPHandler {
public:
  RequestHandler(Arc<MessageQueue> message_queue,
                 Arc<Atomic<uint64_t>> request_id_gen,
                 Arc<Semaphore> active_connections)
    : message_queue_{std::move(message_queue)},
      request_id_gen_{std::move(request_id_gen)},
      active_connections_{std::move(active_connections)} {
  }

  auto
  handleRequest(folly::EventBase* evb, proxygen::coro::HTTPSessionContextPtr,
                proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    auto header_event = co_await request_source.readHeaderEvent();
    auto request = std::move(header_event.headers);
    TENZIR_ASSERT(request);
    if (request->getMethod() != proxygen::HTTPMethod::GET) {
      co_return http_server::make_response(405, "text/plain",
                                           "method not allowed");
    }
    auto permit = co_await active_connections_->acquire();
    auto request_id
      = request_id_gen_->fetch_add(uint64_t{1}, std::memory_order_relaxed);
    auto client = Client::make(request_id, evb, std::move(permit));
    client->start();
    auto* source = client->source();
    TENZIR_ASSERT(source);
    co_await message_queue_->enqueue(ClientStarted{client});
    co_return proxygen::coro::HTTPSourceHolder{source};
  }

private:
  Arc<MessageQueue> message_queue_;
  Arc<Atomic<uint64_t>> request_id_gen_;
  Arc<Semaphore> active_connections_;
};

class ServeHttp final : public Operator<table_slice, void> {
public:
  explicit ServeHttp(ServeHttpArgs args)
    : args_{std::move(args)},
      active_connections_{std::in_place,
                          detail::narrow<size_t>(args_.get_max_connections())} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto config = co_await make_config(ctx);
    if (not config) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto request_id_gen = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
    auto request_handler = std::make_shared<RequestHandler>(
      message_queue_, request_id_gen, active_connections_);
    try {
      auto server = proxygen::coro::ScopedHTTPServer::start(
        std::move(config.unwrap()), std::move(request_handler));
      server_ = Arc<proxygen::coro::ScopedHTTPServer>::from_non_null(
        std::move(server));
    } catch (std::exception const& ex) {
      diagnostic::error("failed to start HTTP server: {}", ex.what())
        .primary(args_.endpoint)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    ctx.spawn_task([this]() -> Task<void> {
      co_await catch_cancellation(wait_forever());
      co_await request_stop(false);
    });
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      co_await request_stop(false);
      co_return;
    }
    co_await ctx.spawn_sub<table_slice>(sub_key_, std::move(pipeline));
    lifecycle_ = Lifecycle::running;
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto* message_ptr = result.try_as<Message>();
    if (not message_ptr) {
      co_return;
    }
    co_await co_match(
      std::move(*message_ptr),
      [&](ClientStarted started) -> Task<void> {
        cleanup_closed_clients();
        if (not started.client or started.client->closed()) {
          co_return;
        }
        if (lifecycle_ != Lifecycle::running) {
          co_await close_client(*started.client, false);
          co_return;
        }
        clients_.push_back(std::move(started.client));
      },
      [&](Payload payload) -> Task<void> {
        cleanup_closed_clients();
        if (lifecycle_ == Lifecycle::done or not payload.chunk
            or payload.chunk->size() == 0 or clients_.empty()) {
          co_return;
        }
        co_await broadcast_payload(payload.chunk);
      });
    maybe_finish_draining();
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (not sub) {
      co_await request_stop(false);
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      co_await request_stop(false);
    }
    co_return;
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done or not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await message_queue_->enqueue(Payload{std::move(chunk)});
    co_return;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    auto graceful = lifecycle_ == Lifecycle::draining;
    if (lifecycle_ == Lifecycle::running) {
      begin_draining();
    }
    co_await close_all_clients(graceful);
    maybe_finish_draining();
    co_return;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      begin_draining();
      if (auto sub = ctx.get_sub(make_view(sub_key_))) {
        auto& pipeline = as<SubHandle<table_slice>>(*sub);
        co_await pipeline.close();
      } else {
        co_await request_stop(false);
      }
    }
    maybe_finish_draining();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await request_stop(false);
    co_return;
  }

  auto state() -> OperatorState override {
    cleanup_closed_clients();
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  auto begin_draining() -> void {
    if (lifecycle_ != Lifecycle::running) {
      return;
    }
    lifecycle_ = Lifecycle::draining;
    if (server_) {
      (*server_)->getServer().drain();
    }
  }

  auto cleanup_closed_clients() -> void {
    std::erase_if(clients_, [](std::shared_ptr<Client> const& client) {
      return not client or client->closed();
    });
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ != Lifecycle::draining) {
      return;
    }
    cleanup_closed_clients();
    if (active_connections_->available_permits()
        != detail::narrow<size_t>(args_.get_max_connections())) {
      return;
    }
    if (clients_.empty() and message_queue_->empty()) {
      server_ = None{};
      lifecycle_ = Lifecycle::done;
    }
  }

  static auto close_client(Client& client, bool graceful) -> Task<void> {
    if (client.closing or client.closed()) {
      co_return;
    }
    client.closing = true;
    auto source_holder = client.shared_from_this();
    co_await folly::coro::co_withExecutor(
      client.evb(), folly::coro::co_invoke([&]() -> Task<void> {
        auto* source = source_holder->source();
        if (not source) {
          co_return;
        }
        if (graceful) {
          if (not client.headers_sent) {
            source->headers(make_response_headers(200, default_content_type),
                            true);
          } else {
            source->eom();
          }
          co_return;
        }
        if (not client.headers_sent) {
          source->headers(make_response_headers(503, default_content_type),
                          true);
        } else {
          source->abort(proxygen::coro::HTTPErrorCode::CANCEL,
                        "server shutting down");
        }
      }));
    co_return;
  }

  auto close_all_clients(bool graceful) -> Task<void> {
    for (auto& client : clients_) {
      if (client) {
        co_await close_client(*client, graceful);
      }
    }
    co_return;
  }
  auto broadcast_payload(chunk_ptr const& chunk) -> Task<void> {
    auto content_type = chunk->metadata().content_type.value_or(
      std::string{default_content_type});
    auto slow_clients = std::vector<std::shared_ptr<Client>>{};
    for (auto& client : clients_) {
      if (not client or client->closing or client->closed()) {
        continue;
      }
      auto ok = co_await write_payload(*client, chunk, content_type);
      if (not ok) {
        slow_clients.push_back(client);
      }
    }
    for (auto& client : slow_clients) {
      co_await close_client(*client, false);
    }
    cleanup_closed_clients();
    co_return;
  }

  static auto write_payload(Client& client, chunk_ptr const& chunk,
                            std::string_view content_type) -> Task<bool> {
    if (client.closing or client.closed()) {
      co_return false;
    }
    auto source_holder = client.shared_from_this();
    co_return co_await folly::coro::co_withExecutor(
      client.evb(), folly::coro::co_invoke([&]() -> Task<bool> {
        auto* source = source_holder->source();
        if (not source) {
          co_return false;
        }
        if (not client.headers_sent) {
          source->headers(make_response_headers(200, content_type));
          client.headers_sent = true;
        }
        auto state = source->body(make_body_queue(chunk), 0);
        if (state == proxygen::coro::HTTPStreamSource::FlowControlState::OPEN) {
          co_return true;
        }
        client.closing = true;
        co_return false;
      }));
  }

  auto request_stop(bool graceful) -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    begin_draining();
    co_await close_all_clients(graceful);
    maybe_finish_draining();
  }

  auto make_config(OpCtx& ctx) const
    -> Task<Option<proxygen::coro::HTTPServer::Config>> {
    auto parsed = http_server::parse_endpoint(args_.endpoint.inner,
                                              args_.endpoint.source, ctx.dh());
    if (not parsed) {
      co_return None{};
    }
    auto tls_enabled = http_server::is_tls_enabled(args_.tls);
    if (parsed->scheme_tls) {
      if (*parsed->scheme_tls and not tls_enabled) {
        diagnostic::error("`https://` endpoint requires `tls=true`")
          .primary(args_.endpoint)
          .emit(ctx);
        co_return None{};
      }
      if (not *parsed->scheme_tls and tls_enabled) {
        diagnostic::error("`http://` endpoint requires `tls=false`")
          .primary(args_.endpoint)
          .emit(ctx);
        co_return None{};
      }
    }
    auto config = proxygen::coro::HTTPServer::Config{};
    try {
      config.socketConfig.bindAddress.setFromHostPort(parsed->host,
                                                      parsed->port);
    } catch (std::exception const& ex) {
      diagnostic::error("failed to configure HTTP endpoint: {}", ex.what())
        .primary(args_.endpoint)
        .emit(ctx);
      co_return None{};
    }
    config.numIOThreads = 1;
    config.sessionConfig.connIdleTimeout = std::chrono::milliseconds{200};
    if (tls_enabled) {
      auto tls_config = http::make_folly_tls_config(
        args_.tls, args_.endpoint.source, ctx.dh(),
        {.tls_default = false, .is_server = true});
      if (not tls_config) {
        co_return None{};
      }
      config.socketConfig.sslContextConfigs.emplace_back(
        std::move(*tls_config));
    }
    co_return config;
  }

  // --- args ---
  ServeHttpArgs args_;
  // --- transient ---
  data sub_key_ = data{int64_t{0}};
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Arc<proxygen::coro::ScopedHTTPServer>> server_;
  Arc<Semaphore> active_connections_;
  std::vector<std::shared_ptr<Client>> clients_;
  // --- state ---
  Lifecycle lifecycle_ = Lifecycle::running;
};

class ServeHttpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.serve_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<ServeHttpArgs, ServeHttp>{};
    auto endpoint_arg = d.positional("endpoint", &ServeHttpArgs::endpoint);
    auto max_connections_arg
      = d.named("max_connections", &ServeHttpArgs::max_connections);
    auto tls_validator
      = tls_options{{.tls_default = false, .is_server = true}}.add_to_describer(
        d, &ServeHttpArgs::tls);
    auto printer_pipeline = d.pipeline(&ServeHttpArgs::printer);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      if (auto endpoint = ctx.get(endpoint_arg)) {
        std::ignore
          = http_server::parse_endpoint(endpoint->inner, endpoint->source, ctx);
      }
      if (auto max_connections = ctx.get(max_connections_arg);
          max_connections) {
        if (max_connections->inner == 0) {
          diagnostic::error("`max_connections` must be greater than 0")
            .primary(max_connections->source)
            .emit(ctx);
        } else if (max_connections->inner > static_cast<uint64_t>(
                     std::numeric_limits<size_t>::max())) {
          diagnostic::error("`max_connections` is too large")
            .primary(max_connections->source)
            .note("maximum supported value: {}",
                  std::numeric_limits<size_t>::max())
            .emit(ctx);
        }
      }
      TRY(auto printer, ctx.get(printer_pipeline));
      auto output = printer.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(printer.source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::serve_http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::serve_http::ServeHttpPlugin)
