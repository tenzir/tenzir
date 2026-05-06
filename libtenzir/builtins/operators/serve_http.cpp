//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/join_set.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/box.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
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
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace tenzir::plugins::serve_http {

namespace {

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

class Client;

struct ClientStarted {
  Arc<Client> client;
};

struct Payload {
  chunk_ptr chunk;
};

struct EndOfStream {};

struct ClientClosed {};

using Message = variant<Payload, ClientStarted, EndOfStream, ClientClosed>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

class Client final : public proxygen::coro::HTTPStreamSource::Callback {
public:
  static auto make(folly::EventBase* evb, SemaphorePermit permit,
                   Arc<MessageQueue> message_queue) -> Arc<Client> {
    return Arc<Client>::from_non_null(std::shared_ptr<Client>{
      new Client{evb, std::move(permit), std::move(message_queue)}});
  }

  Client(const Client&) = delete;
  Client(Client&&) = delete;
  Client& operator=(const Client&) = delete;
  Client& operator=(Client&&) = delete;
  ~Client() override {
    // `HTTPStreamSource` must be destroyed on its event-base thread. We only
    // release it from `sourceComplete()`, which proxygen invokes on that
    // thread, so asserting here is safe and catches lifetime regressions.
    TENZIR_ASSERT(source_.is_none());
  }

  auto start(Arc<Client> self) -> void {
    lib_ref_ = std::move(self);
  }

  auto cancel_startup() noexcept -> void {
    // `handleRequest()` runs on the EventBase thread, so destroying the source
    // here is safe if startup is cancelled before proxygen takes ownership via
    // the returned `HTTPSourceHolder`.
    lifecycle_.store(Lifecycle::closed, std::memory_order_release);
    writable_.notify_one();
    permit_ = None{};
    source_.reset();
    lib_ref_ = None{};
  }

  auto source() -> proxygen::coro::HTTPStreamSource* {
    return source_ ? &**source_ : nullptr;
  }

  auto source() const -> proxygen::coro::HTTPStreamSource const* {
    return source_ ? &**source_ : nullptr;
  }

  auto evb() const -> folly::EventBase* {
    return evb_;
  }

  auto closed() const -> bool {
    return lifecycle_.load(std::memory_order_acquire) == Lifecycle::closed;
  }

  auto closing() const -> bool {
    auto lifecycle = lifecycle_.load(std::memory_order_acquire);
    return lifecycle == Lifecycle::aborting or lifecycle == Lifecycle::closed;
  }

  auto close(bool graceful) -> void {
    if (closed()) {
      return;
    }
    if (not graceful) {
      lifecycle_.store(Lifecycle::aborting, std::memory_order_release);
      writable_.notify_one();
      auto* source = this->source();
      if (not source) {
        return;
      }

      source->abort(proxygen::coro::HTTPErrorCode::CANCEL,
                    "server shutting down");
    } else {
      auto lifecycle = lifecycle_.load(std::memory_order_acquire);
      if (lifecycle == Lifecycle::aborting or lifecycle == Lifecycle::closed) {
        return;
      }
      if (lifecycle == Lifecycle::open
          or lifecycle == Lifecycle::headers_sent) {
        lifecycle_.store(Lifecycle::graceful_close_pending,
                         std::memory_order_release);
      }
      maybe_finish_graceful_close();
    }
  }

  auto write_payload(chunk_ptr const& chunk, std::string_view content_type)
    -> Task<bool> {
    while (flow_control_blocked_) {
      co_await writable_.wait();
      if (closing()) {
        co_return false;
      }
    }
    auto* source = this->source();
    if (not source or closing()) {
      co_return false;
    }
    write_in_progress_ = true;
    if (not response_started_) {
      source->headers(make_response_headers(200, content_type));
      response_started_ = true;
      lifecycle_.store(Lifecycle::headers_sent, std::memory_order_relaxed);
    }
    auto state = source->body(make_body_queue(chunk), 0);
    write_in_progress_ = false;
    if (state == proxygen::coro::HTTPStreamSource::FlowControlState::ERROR) {
      co_return false;
    }
    flow_control_blocked_
      = state == proxygen::coro::HTTPStreamSource::FlowControlState::CLOSED;
    maybe_finish_graceful_close();
    co_return true;
  }

  void sourceComplete(proxygen::HTTPCodec::StreamID,
                      folly::Optional<proxygen::coro::HTTPError>) override {
    lifecycle_.store(Lifecycle::closed, std::memory_order_release);
    writable_.notify_one();
    permit_ = None{};
    source_.reset();
    // Wake the operator loop so draining can observe the returned permit and
    // transition to done even when no further payloads arrive. Ignoring a
    // failed enqueue is safe: the bounded queue only rejects when it is full,
    // so there is already queued work that will drive `process_task()` and
    // `maybe_finish_draining()` again. If the queue is empty or merely
    // non-full, this enqueue succeeds and provides the wakeup itself. Keep the
    // self reference alive until after the enqueue finishes.
    std::ignore = message_queue_->try_enqueue(ClientClosed{});
    lib_ref_ = None{};
  }

  void windowOpen(proxygen::HTTPCodec::StreamID) override {
    flow_control_blocked_ = false;
    maybe_finish_graceful_close();
    writable_.notify_one();
  }

private:
  enum class Lifecycle {
    open,
    headers_sent,
    graceful_close_pending,
    graceful_closing,
    aborting,
    closed,
  };

  auto maybe_finish_graceful_close() -> void {
    if (write_in_progress_ or flow_control_blocked_) {
      return;
    }
    auto lifecycle = lifecycle_.load(std::memory_order_acquire);
    if (lifecycle != Lifecycle::graceful_close_pending) {
      return;
    }
    auto* source = this->source();
    if (not source) {
      return;
    }
    auto had_headers = response_started_;
    lifecycle_.store(Lifecycle::graceful_closing, std::memory_order_release);
    if (had_headers) {
      source->eom();
    } else {
      source->headers(make_response_headers(200, ""), true);
      response_started_ = true;
    }
  }

  explicit Client(folly::EventBase* evb, SemaphorePermit permit,
                  Arc<MessageQueue> message_queue)
    : evb_{evb},
      source_{std::in_place, std::in_place, evb,
              folly::none,   this,          egress_buffer_size},
      permit_{std::move(permit)},
      message_queue_{std::move(message_queue)} {
  }

  folly::EventBase* evb_ = nullptr;
  Option<Box<proxygen::coro::HTTPStreamSource>> source_;
  Option<SemaphorePermit> permit_;
  Arc<MessageQueue> message_queue_;
  Option<Arc<Client>> lib_ref_;
  Atomic<Lifecycle> lifecycle_ = Lifecycle::open;
  bool response_started_ = false;
  bool write_in_progress_ = false;
  bool flow_control_blocked_ = false;
  Notify writable_;
};

class RequestHandler final : public proxygen::coro::HTTPHandler {
public:
  RequestHandler(Arc<MessageQueue> message_queue,
                 Arc<Semaphore> active_connections)
    : message_queue_{std::move(message_queue)},
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
    auto client = Client::make(evb, std::move(permit), message_queue_);
    client->start(client);
    auto startup_guard = detail::scope_guard{[client]() mutable noexcept {
      client->cancel_startup();
    }};
    auto* source = client->source();
    TENZIR_ASSERT(source);
    co_await message_queue_->enqueue(ClientStarted{client});
    startup_guard.disable();
    co_return proxygen::coro::HTTPSourceHolder{source};
  }

private:
  Arc<MessageQueue> message_queue_;
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
    auto request_handler
      = std::make_shared<RequestHandler>(message_queue_, active_connections_);
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
      co_await force_stop();
    });
    co_await ctx.spawn_sub<table_slice>(sub_key_,
                                        std::move(args_.printer.inner));
    lifecycle_ = Lifecycle::running;
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (not sub) {
      co_await force_stop();
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      co_await force_stop();
    }
    co_return;
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await message_queue_->enqueue(Payload{std::move(chunk)});
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx&) -> Task<void> override {
    auto* message_ptr = result.try_as<Message>();
    if (not message_ptr) {
      co_return;
    }
    co_await co_match(
      std::move(*message_ptr),
      [&](ClientStarted started) -> Task<void> {
        cleanup_closed_clients();
        if (started.client->closed()) {
          co_return;
        }
        if (lifecycle_ != Lifecycle::running) {
          auto graceful = lifecycle_ == Lifecycle::draining;
          co_await folly::coro::co_withExecutor(
            started.client->evb(),
            folly::coro::co_invoke(
              [client = started.client, graceful]() mutable -> Task<void> {
                client->close(graceful);
                co_return;
              }));
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
      },
      [&](EndOfStream) -> Task<void> {
        cleanup_closed_clients();
        if (lifecycle_ == Lifecycle::running) {
          begin_draining();
        }
        co_await close_all_clients(true);
      },
      [&](ClientClosed) -> Task<void> {
        cleanup_closed_clients();
        co_return;
      });
    maybe_finish_draining();
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    co_await message_queue_->enqueue(EndOfStream{});
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
        co_await force_stop();
      }
    }
    maybe_finish_draining();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await force_stop();
    co_return;
  }

  auto state() -> OperatorState override {
    cleanup_closed_clients();
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  auto force_stop() -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    begin_draining();
    co_await close_all_clients(false);
    maybe_finish_draining();
  }

  auto begin_draining() -> void {
    if (lifecycle_ != Lifecycle::running) {
      return;
    }
    lifecycle_ = Lifecycle::draining;
    if (server_) {
      (*server_)->getServer().drain();
    }
  }

  auto close_all_clients(bool graceful) -> Task<void> {
    auto join = JoinSet<bool>{};
    co_await join.activate([&]() -> Task<void> {
      for (auto& client : clients_) {
        join.add([client, graceful]() mutable -> Task<bool> {
          co_return co_await folly::coro::co_withExecutor(
            client->evb(),
            folly::coro::co_invoke([client, graceful]() mutable -> Task<bool> {
              client->close(graceful);
              co_return true;
            }));
        });
      }
      while (co_await join.next()) {
      }
    });
    co_return;
  }

  auto cleanup_closed_clients() -> void {
    std::erase_if(clients_, [](Arc<Client> const& client) {
      return client->closed();
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
      if (server_) {
        (*server_)->getServer().forceStop();
        // move server to a new thread, where it can call thread join
        std::thread([srv = std::exchange(server_, None{})] {}).detach();
      }
      lifecycle_ = Lifecycle::done;
    }
  }

  auto broadcast_payload(chunk_ptr const& chunk) -> Task<void> {
    auto content_type = chunk->metadata().content_type.value_or("");
    auto join = JoinSet<bool>{};
    co_await join.activate([&]() -> Task<void> {
      // send payloads to each client
      for (auto& client : clients_) {
        if (client->closing()) {
          continue;
        }
        join.add([client, chunk, content_type]() mutable -> Task<bool> {
          co_return co_await folly::coro::co_withExecutor(
            client->evb(),
            folly::coro::co_invoke(
              [client, chunk, content_type]() mutable -> Task<bool> {
                co_return co_await client->write_payload(chunk, content_type);
              }));
        });
      }
      // wait for all; the slowest client determines progress.
      while (co_await join.next()) {
      }
    });
    cleanup_closed_clients();
    co_return;
  }

  auto make_config(OpCtx& ctx) const
    -> Task<Option<proxygen::coro::HTTPServer::Config>> {
    auto parsed = http_server::parse_endpoint(args_.endpoint.inner,
                                              args_.endpoint.source, ctx.dh());
    if (not parsed) {
      co_return None{};
    }
    auto const* cfg = std::addressof(ctx.actor_system().config());
    auto tls_enabled = http_server::is_tls_enabled(args_.tls, cfg);
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
    if (tls_enabled) {
      auto tls_opts = tls_options::from_optional(
        args_.tls, {.tls_default = false, .is_server = true});
      auto tls_config = http_server::make_ssl_context_config(
        tls_opts, args_.endpoint.source, ctx.dh(), cfg);
      if (not tls_config) {
        co_return None{};
      }
      config.socketConfig.sslContextConfigs.emplace_back(
        std::move(*tls_config));
    }
    config.shutdownOnSignals = {};
    co_return config;
  }

  // --- args ---
  ServeHttpArgs args_;
  // --- transient ---
  data sub_key_ = data{int64_t{0}};
  mutable Arc<MessageQueue> message_queue_{std::in_place, 64};
  Option<Arc<proxygen::coro::ScopedHTTPServer>> server_;
  Arc<Semaphore> active_connections_;
  std::vector<Arc<Client>> clients_;
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
      if (auto max_connections = ctx.get(max_connections_arg)) {
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
