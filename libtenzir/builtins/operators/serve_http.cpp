//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/unbounded_queue.hpp"
#include "tenzir/box.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/http_server.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <folly/ScopeGuard.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Sleep.h>
#include <folly/fibers/Semaphore.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <proxygen/lib/services/AcceptorConfiguration.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins {
namespace {

using namespace tenzir::si_literals;

constexpr auto message_queue_capacity = uint32_t{1_Ki};
constexpr auto default_stream_path = std::string_view{"/"};
constexpr auto default_stream_method = std::string_view{"GET"};
constexpr auto default_stream_content_type
  = std::string_view{"application/x-ndjson"};
constexpr auto http_1_1 = std::string_view{"http/1.1"};
constexpr auto backlog_grace_period = std::chrono::seconds{1};

template <typename T>
constexpr auto inner(std::optional<located<T>> const& x) -> std::optional<T> {
  return x.transform([](auto&& x) {
    return x.inner;
  });
}

enum class BacklogPolicy : uint8_t {
  buffer,
  block,
  drop,
};

auto parse_backlog_policy(std::string_view value)
  -> std::optional<BacklogPolicy> {
  if (detail::ascii_icase_equal(value, "buffer")) {
    return BacklogPolicy::buffer;
  }
  if (detail::ascii_icase_equal(value, "block")) {
    return BacklogPolicy::block;
  }
  if (detail::ascii_icase_equal(value, "drop")) {
    return BacklogPolicy::drop;
  }
  return std::nullopt;
}

struct ServeHttpArgs {
  location op = location::unknown;
  located<secret> url;
  std::optional<located<std::string>> path;
  std::optional<located<std::string>> method;
  std::optional<located<record>> responses;
  std::optional<located<std::string>> on_backlog;
  located<uint64_t> max_connections{0, location::unknown};
  std::optional<located<data>> tls;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (path and path->inner.empty()) {
      diagnostic::error("`path` must not be empty").primary(*path).emit(dh);
      return failure::promise();
    }
    if (method) {
      if (not http::normalize_http_method(method->inner)) {
        diagnostic::error("unsupported http method: `{}`", method->inner)
          .primary(*method)
          .emit(dh);
        return failure::promise();
      }
    }
    auto stream_path = path ? path->inner : std::string{default_stream_path};
    if (responses) {
      TRY(http::validate_response_map(responses->inner, dh, responses->source));
      if (responses->inner.find(stream_path) != responses->inner.end()) {
        diagnostic::error("`responses` must not define the stream `path` route")
          .primary(*responses)
          .emit(dh);
        return failure::promise();
      }
    }
    if (on_backlog and not parse_backlog_policy(on_backlog->inner)) {
      diagnostic::error("unsupported backlog policy: `{}`", on_backlog->inner)
        .primary(*on_backlog)
        .hint("`on_backlog` must be `\"buffer\"`, `\"block\"`, or `\"drop\"`")
        .emit(dh);
      return failure::promise();
    }
    if (max_connections.inner == 0) {
      diagnostic::error("`max_connections` must not be zero")
        .primary(max_connections.source)
        .emit(dh);
      return failure::promise();
    }
    if (max_connections.inner > detail::max_http_server_connections) {
      diagnostic::error("`max_connections` must be at most {}",
                        detail::max_http_server_connections)
        .primary(max_connections.source)
        .emit(dh);
      return failure::promise();
    }
    auto tls_opts
      = tls ? tls_options{*tls, {.tls_default = false, .is_server = true}}
            : tls_options{{.tls_default = false, .is_server = true}};
    TRY(tls_opts.validate(dh));
    return {};
  }
};

struct ServeHttpClient {
  Arc<UnboundedQueue<Option<std::string>>> queue{std::in_place};
};

using ClientKey = const ServeHttpClient*;

auto client_key(Arc<ServeHttpClient> const& client) -> ClientKey {
  return client.operator->();
}

struct ClientOpened {
  Arc<ServeHttpClient> client;
};

struct ClientClosed {
  ClientKey client = nullptr;
};

struct Payload {
  std::string line;
};

struct Shutdown {};

struct BacklogTimeout {};

using Message
  = variant<Shutdown, ClientClosed, Payload, BacklogTimeout, ClientOpened>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

enum class Lifecycle {
  running,
  draining,
  done,
};

enum class ListenerState {
  accepting,
  stopping,
};

struct ServeHttpServerState {
  ServeHttpServerState(Arc<MessageQueue> message_queue,
                       Option<record> responses, std::string stream_path,
                       std::string stream_method, uint64_t max_connections)
    : message_queue{std::move(message_queue)},
      responses_{std::move(responses)},
      stream_path_{std::move(stream_path)},
      stream_method_{std::move(stream_method)},
      connection_slots_{std::in_place,
                        detail::narrow<size_t>(max_connections)} {
  }

  auto responses() const -> Option<record> const& {
    return responses_;
  }

  auto stream_path() const -> std::string const& {
    return stream_path_;
  }

  auto stream_method() const -> std::string const& {
    return stream_method_;
  }

  auto create_client() -> Option<Arc<ServeHttpClient>> {
    if (not connection_slots_->try_wait()) {
      return None{};
    }
    return Arc<ServeHttpClient>{std::in_place};
  }

  auto release_connection_slot() -> void {
    connection_slots_->signal();
  }

  Arc<MessageQueue> message_queue;

private:
  Option<record> responses_;
  std::string stream_path_ = std::string{default_stream_path};
  std::string stream_method_ = std::string{default_stream_method};

  // The HTTP handler must decide synchronously whether it can hand out a new
  // streaming response before the operator thread processes the corresponding
  // `ClientOpened` message, so a small semaphore-backed token pool is the
  // minimal shared surface that preserves queue-based coordination.
  Box<folly::fibers::Semaphore> connection_slots_;
};

auto notify_client_closed(ClientKey client, Arc<MessageQueue> message_queue)
  -> Task<void> {
  co_await message_queue->enqueue(ClientClosed{client});
}

class ServeHttpStreamSource final : public proxygen::coro::HTTPSource {
public:
  ServeHttpStreamSource(Arc<ServeHttpClient> client, folly::EventBase* evb,
                        Arc<MessageQueue> message_queue)
    : client_{std::move(client)},
      evb_{evb},
      message_queue_{std::move(message_queue)} {
    setHeapAllocated();
  }

  auto readHeaderEvent()
    -> folly::coro::Task<proxygen::coro::HTTPHeaderEvent> override {
    auto msg = std::make_unique<proxygen::HTTPMessage>();
    msg->setStatusCode(200);
    msg->setStatusMessage(proxygen::HTTPMessage::getDefaultReason(200));
    msg->setHTTPVersion(1, 1);
    msg->getHeaders().set("Content-Type",
                          std::string{default_stream_content_type});
    auto event = proxygen::coro::HTTPHeaderEvent{std::move(msg), false};
    auto guard = folly::makeGuard(lifetime(event));
    co_return event;
  }

  auto readBodyEvent(uint32_t max = std::numeric_limits<uint32_t>::max())
    -> folly::coro::Task<proxygen::coro::HTTPBodyEvent> override {
    if (max == 0) {
      max = 1;
    }
    while (buffered_.empty() and not input_done_) {
      auto payload = co_await client_->queue->dequeue();
      if (payload) {
        buffered_ = std::move(*payload);
      } else {
        input_done_ = true;
      }
    }
    if (buffered_.empty()) {
      co_await notify_client_closed(client_key(client_), message_queue_);
      auto event = proxygen::coro::HTTPBodyEvent{
        std::unique_ptr<folly::IOBuf>{nullptr}, true};
      auto guard = folly::makeGuard(lifetime(event));
      co_return event;
    }
    auto chunk_size = std::min<size_t>(max, buffered_.size());
    auto chunk = folly::IOBuf::copyBuffer(
      std::string_view{buffered_.data(), chunk_size});
    buffered_.erase(0, chunk_size);
    auto event = proxygen::coro::HTTPBodyEvent{
      std::move(chunk), input_done_ and buffered_.empty()};
    if (event.eom) {
      co_await notify_client_closed(client_key(client_), message_queue_);
    }
    auto guard = folly::makeGuard(lifetime(event));
    co_return event;
  }

  void
  stopReading(folly::Optional<const proxygen::coro::HTTPErrorCode>) override {
    folly::coro::co_withExecutor(evb_, notify_client_closed(client_key(client_),
                                                            message_queue_))
      .start();
    if (heapAllocated_) {
      delete this;
    }
  }

private:
  Arc<ServeHttpClient> client_;
  folly::EventBase* evb_ = nullptr;
  Arc<MessageQueue> message_queue_;
  std::string buffered_;
  bool input_done_ = false;
};

class ServeHttpHandler final : public proxygen::coro::HTTPHandler {
public:
  explicit ServeHttpHandler(Arc<ServeHttpServerState> state)
    : state_{std::move(state)} {
  }

  auto
  handleRequest(folly::EventBase* evb, proxygen::coro::HTTPSessionContextPtr,
                proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    auto request_path = std::string{};
    auto request_method = std::string{};
    auto reader = proxygen::coro::HTTPSourceReader{std::move(request_source)};
    reader.onHeaders([&](std::unique_ptr<proxygen::HTTPMessage> msg,
                         bool is_final, bool /*eom*/) -> bool {
      if (not is_final) {
        return proxygen::coro::HTTPSourceReader::Continue;
      }
      request_method = msg->getMethodString();
      request_path = msg->getPath();
      return proxygen::coro::HTTPSourceReader::Continue;
    });
    reader.onBody([&](proxygen::coro::BufQueue body, bool /*eom*/) -> bool {
      std::ignore = body.move();
      return proxygen::coro::HTTPSourceReader::Continue;
    });
    co_await reader.read();
    if (auto const& responses = state_->responses(); responses) {
      if (auto response = http::lookup_response(*responses, request_path)) {
        co_return http::make_fixed_response_source(
          response->code, std::move(response->body), response->content_type);
      }
    }
    if (request_path != state_->stream_path()) {
      co_return http::make_fixed_response_source(404, std::string{});
    }
    if (not detail::ascii_icase_equal(request_method,
                                      state_->stream_method())) {
      co_return http::make_fixed_response_source(405, std::string{});
    }
    auto client = state_->create_client();
    if (not client) {
      co_return http::make_fixed_response_source(503, std::string{});
    }
    co_await state_->message_queue->enqueue(ClientOpened{*client});
    co_return proxygen::coro::HTTPSourceHolder{
      new ServeHttpStreamSource{*client, evb, state_->message_queue}};
  }

private:
  Arc<ServeHttpServerState> state_;
};

class ServeHttp final : public Operator<table_slice, void> {
public:
  explicit ServeHttp(ServeHttpArgs args) : args_{std::move(args)} {
  }

  ServeHttp(ServeHttp const&) = delete;
  auto operator=(ServeHttp const&) -> ServeHttp& = delete;
  ServeHttp(ServeHttp&&) noexcept = default;
  auto operator=(ServeHttp&&) noexcept -> ServeHttp& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto request = make_secret_request("url", args_.url, resolved_url_, dh);
    auto resolved = co_await ctx.resolve_secrets({std::move(request)});
    if (not resolved) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (resolved_url_.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto endpoint = http::parse_host_port_endpoint(resolved_url_);
    if (endpoint.is_err()) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    tls_options_
      = args_.tls
          ? tls_options{*args_.tls, {.tls_default = false, .is_server = true}}
          : tls_options{{.tls_default = false, .is_server = true}};
    auto stream_method = std::string{default_stream_method};
    if (args_.method) {
      auto method = http::normalize_http_method(args_.method->inner);
      TENZIR_ASSERT(method);
      stream_method = std::move(*method);
    }
    server_state_ = Arc<ServeHttpServerState>{
      std::in_place,
      message_queue_,
      args_.responses.transform([](auto const& x) {
        return x.inner;
      }),
      args_.path ? args_.path->inner : std::string{default_stream_path},
      std::move(stream_method),
      args_.max_connections.inner,
    };
    if (args_.on_backlog) {
      auto policy = parse_backlog_policy(args_.on_backlog->inner);
      TENZIR_ASSERT(policy);
      backlog_policy_ = *policy;
    }
    handler_ = std::make_shared<ServeHttpHandler>(*server_state_);
    auto [host, port] = std::move(endpoint).unwrap();
    auto config = make_server_config(host, port, dh);
    if (not config) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    server_ = Box<::tenzir::detail::HttpServerRunner>{
      std::in_place, folly::SocketAddress{host, port, true}, std::move(*config),
      handler_};
    if (auto error = (*server_)->start()) {
      diagnostic::error("failed to start http server")
        .primary(args_.url)
        .note("reason: {}", *error)
        .emit(dh);
      server_ = None{};
      lifecycle_ = Lifecycle::done;
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](ClientOpened event) -> Task<void> {
        if (lifecycle_ == Lifecycle::done
            or (lifecycle_ == Lifecycle::draining
                and pending_payloads_.empty())) {
          event.client->queue->enqueue(None{});
          TENZIR_ASSERT(server_state_);
          (*server_state_)->release_connection_slot();
          co_return;
        }
        auto [_, inserted]
          = clients_.emplace(client_key(event.client), std::move(event.client));
        TENZIR_ASSERT(inserted);
        client_ready_->notify_one();
        if (not pending_payloads_.empty()) {
          for (auto const& line : pending_payloads_) {
            send_to_clients(line);
          }
          pending_payloads_.clear();
          if (lifecycle_ == Lifecycle::draining) {
            stop_accepting();
            close_all_clients();
          }
        }
        maybe_finish_draining();
        co_return;
      },
      [&](ClientClosed event) -> Task<void> {
        if (clients_.erase(event.client) == 1) {
          TENZIR_ASSERT(server_state_);
          (*server_state_)->release_connection_slot();
        }
        maybe_finish_draining();
        co_return;
      },
      [&](Payload payload) -> Task<void> {
        if (lifecycle_ == Lifecycle::done or payload.line.empty()) {
          co_return;
        }
        if (not clients_.empty()) {
          send_to_clients(payload.line);
          co_return;
        }
        switch (backlog_policy_) {
          case BacklogPolicy::buffer:
          case BacklogPolicy::block:
            pending_payloads_.push_back(std::move(payload.line));
            break;
          case BacklogPolicy::drop:
            break;
        }
        co_return;
      },
      [&](Shutdown) -> Task<void> {
        if (lifecycle_ != Lifecycle::draining) {
          co_return;
        }
        if (clients_.empty()) {
          if (not pending_payloads_.empty()) {
            switch (backlog_policy_) {
              case BacklogPolicy::buffer:
                ctx.spawn_task(
                  [message_queue = message_queue_]() mutable -> Task<void> {
                    co_await folly::coro::sleep(backlog_grace_period);
                    co_await message_queue->enqueue(BacklogTimeout{});
                  });
                co_return;
              case BacklogPolicy::block:
                co_return;
              case BacklogPolicy::drop:
                pending_payloads_.clear();
                break;
            }
          }
          stop_accepting();
          maybe_finish_draining();
          co_return;
        }
        stop_accepting();
        close_all_clients();
        maybe_finish_draining();
        co_return;
      },
      [&](BacklogTimeout) -> Task<void> {
        if (lifecycle_ == Lifecycle::draining and clients_.empty()
            and backlog_policy_ == BacklogPolicy::buffer
            and not pending_payloads_.empty()) {
          pending_payloads_.clear();
          stop_accepting();
          maybe_finish_draining();
        }
        co_return;
      });
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    if (input.rows() == 0) {
      co_return;
    }
    for (auto row : input.values()) {
      if (lifecycle_ != Lifecycle::running) {
        co_return;
      }
      auto json = to_json(materialize(row), {.oneline = true});
      if (not json) {
        diagnostic::warning("failed to encode event as json")
          .note("skipping event")
          .emit(ctx);
        continue;
      }
      auto line = std::move(*json);
      line.push_back('\n');
      co_await message_queue_->enqueue(Payload{std::move(line)});
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    co_await request_shutdown(ctx);
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    co_await request_shutdown(ctx);
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  auto make_server_config(std::string const& host, uint16_t port,
                          diagnostic_handler& dh)
    -> failure_or<std::shared_ptr<const proxygen::AcceptorConfiguration>> {
    auto config = std::make_shared<proxygen::AcceptorConfiguration>();
    config->plaintextProtocol = std::string{http_1_1};
    config->forceHTTP1_0_to_1_1 = true;
    config->bindAddress = folly::SocketAddress{host, port, true};
    config->acceptBacklog
      = detail::narrow<uint32_t>(args_.max_connections.inner);
    config->maxNumPendingConnectionsPerWorker = config->acceptBacklog;
    if (not tls_options_.get_tls(nullptr).inner) {
      return config;
    }
    auto certfile = tls_options_.get_certfile(nullptr);
    auto keyfile = tls_options_.get_keyfile(nullptr);
    if (not certfile or not keyfile) {
      diagnostic::error(
        "TLS server mode requires `tls.certfile` and `tls.keyfile`")
        .primary(args_.url)
        .emit(dh);
      return failure::promise();
    }
    auto tls_config = proxygen::coro::HTTPServer::getDefaultTLSConfig();
    try {
      tls_config.setCertificate(certfile->inner, keyfile->inner, "");
    } catch (std::exception const& ex) {
      diagnostic::error("failed to configure TLS server certificate")
        .primary(args_.url)
        .note("reason: {}", ex.what())
        .emit(dh);
      return failure::promise();
    }
    config->sslContextConfigs.emplace_back(std::move(tls_config));
    return config;
  }

  auto stop_accepting() -> void {
    if (listener_state_ == ListenerState::stopping) {
      return;
    }
    listener_state_ = ListenerState::stopping;
    if (server_) {
      (*server_)->request_stop_accepting();
    }
  }

  auto wait_for_server() -> void {
    server_ = None{};
    handler_.reset();
    server_state_ = None{};
  }

  auto send_to_clients(std::string const& line) -> void {
    for (auto& [_, client] : clients_) {
      client->queue->enqueue(line);
    }
  }

  auto close_all_clients() -> void {
    for (auto& [_, client] : clients_) {
      client->queue->enqueue(None{});
    }
  }

  auto request_shutdown(OpCtx&) -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      client_ready_->notify_one();
      co_await message_queue_->enqueue(Shutdown{});
    }
    maybe_finish_draining();
    co_return;
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ == Lifecycle::draining
        and listener_state_ == ListenerState::stopping and clients_.empty()) {
      wait_for_server();
      lifecycle_ = Lifecycle::done;
    }
  }

  ServeHttpArgs args_;
  std::string resolved_url_;
  mutable Arc<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Option<Arc<ServeHttpServerState>> server_state_;
  Option<Box<::tenzir::detail::HttpServerRunner>> server_;
  std::shared_ptr<ServeHttpHandler> handler_;
  std::unordered_map<ClientKey, Arc<ServeHttpClient>> clients_;
  std::vector<std::string> pending_payloads_;
  Box<Notify> client_ready_{std::in_place};
  tls_options tls_options_{{.tls_default = false, .is_server = true}};
  BacklogPolicy backlog_policy_ = BacklogPolicy::buffer;
  Lifecycle lifecycle_ = Lifecycle::running;
  ListenerState listener_state_ = ListenerState::accepting;
};

} // namespace

struct ServeHttpPlugin final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.serve_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<ServeHttpArgs, ServeHttp>{};
    d.operator_location(&ServeHttpArgs::op);
    auto url = d.positional("url", &ServeHttpArgs::url);
    auto path = d.named("path", &ServeHttpArgs::path, "string");
    auto method = d.named("method", &ServeHttpArgs::method, "string");
    auto responses = d.named("responses", &ServeHttpArgs::responses);
    auto on_backlog
      = d.named("on_backlog", &ServeHttpArgs::on_backlog, "string");
    auto max_connections
      = d.named("max_connections", &ServeHttpArgs::max_connections);
    auto tls = d.named("tls", &ServeHttpArgs::tls);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto args = ServeHttpArgs{};
      args.op = ctx.get_location(url).value_or(location::unknown);
      if (auto x = ctx.get(url)) {
        args.url = *x;
      }
      if (auto x = ctx.get(path)) {
        args.path = *x;
      }
      if (auto x = ctx.get(method)) {
        args.method = *x;
      }
      if (auto x = ctx.get(responses)) {
        args.responses = *x;
      }
      if (auto x = ctx.get(on_backlog)) {
        args.on_backlog = *x;
      }
      if (auto x = ctx.get(max_connections)) {
        args.max_connections = *x;
      } else {
        diagnostic::error("`max_connections` is required")
          .primary(args.op)
          .emit(ctx);
      }
      if (auto x = ctx.get(tls)) {
        args.tls = *x;
      }
      std::ignore = args.validate(ctx);
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ServeHttpPlugin)
