//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/async.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/tls.hpp"
#include "tenzir/async/unbounded_queue.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <folly/ScopeGuard.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/coro/ServerSocket.h>
#define nsel_CONFIG_SELECT_EXPECTED 1
#include <proxygen/lib/http/codec/HTTPCodecFactory.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>

#include <atomic>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins {
namespace {

constexpr auto default_listen_backlog = uint32_t{128};
constexpr auto default_stream_path = std::string_view{"/"};
constexpr auto default_stream_method = std::string_view{"GET"};
constexpr auto default_stream_content_type
  = std::string_view{"application/x-ndjson"};

template <typename T>
constexpr auto inner(std::optional<located<T>> const& x) -> std::optional<T> {
  return x.transform([](auto&& x) {
    return x.inner;
  });
}

class ServeHttpClientRegistry;

class ServeHttpClient final {
public:
  ServeHttpClient(uint64_t id, std::weak_ptr<ServeHttpClientRegistry> registry)
    : id_{id}, registry_{std::move(registry)} {
  }

  auto enqueue(std::string_view payload) -> bool {
    if (closed_.load(std::memory_order_acquire)) {
      return false;
    }
    queue_->enqueue(std::optional<std::string>{std::string{payload}});
    return true;
  }

  auto dequeue() const -> Task<std::optional<std::string>> {
    co_return co_await queue_->dequeue();
  }

  auto close() -> void;

private:
  uint64_t id_ = 0;
  std::weak_ptr<ServeHttpClientRegistry> registry_;
  std::shared_ptr<UnboundedQueue<std::optional<std::string>>> queue_
    = std::make_shared<UnboundedQueue<std::optional<std::string>>>();
  std::atomic<bool> closed_ = false;
};

class ServeHttpClientRegistry final
  : public std::enable_shared_from_this<ServeHttpClientRegistry> {
public:
  auto create_client() -> std::shared_ptr<ServeHttpClient> {
    auto guard = std::lock_guard{mutex_};
    if (stopping_) {
      return {};
    }
    auto id = next_client_id_++;
    auto client = std::make_shared<ServeHttpClient>(id, weak_from_this());
    clients_.emplace(id, client);
    clients_changed_.notify_one();
    return client;
  }

  auto remove_client(uint64_t id) -> void {
    {
      auto guard = std::lock_guard{mutex_};
      clients_.erase(id);
    }
    clients_changed_.notify_one();
  }

  auto snapshot_clients() const
    -> std::vector<std::shared_ptr<ServeHttpClient>> {
    auto guard = std::lock_guard{mutex_};
    auto result = std::vector<std::shared_ptr<ServeHttpClient>>{};
    result.reserve(clients_.size());
    for (auto const& [_, client] : clients_) {
      result.push_back(client);
    }
    return result;
  }

  auto has_clients() const -> bool {
    auto guard = std::lock_guard{mutex_};
    return not clients_.empty();
  }

  auto is_stopping() const -> bool {
    auto guard = std::lock_guard{mutex_};
    return stopping_;
  }

  auto wait_for_change() -> Task<void> {
    co_await clients_changed_.wait();
  }

  auto shutdown() -> void {
    auto clients = std::vector<std::shared_ptr<ServeHttpClient>>{};
    {
      auto guard = std::lock_guard{mutex_};
      if (stopping_) {
        return;
      }
      stopping_ = true;
      clients.reserve(clients_.size());
      for (auto const& [_, client] : clients_) {
        clients.push_back(client);
      }
      clients_.clear();
    }
    for (auto const& client : clients) {
      client->close();
    }
    clients_changed_.notify_one();
  }

private:
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<ServeHttpClient>> clients_;
  uint64_t next_client_id_ = 0;
  bool stopping_ = false;
  Notify clients_changed_;
};

auto ServeHttpClient::close() -> void {
  auto expected = false;
  if (not closed_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
    return;
  }
  queue_->enqueue(std::nullopt);
  if (auto registry = registry_.lock()) {
    registry->remove_client(id_);
  }
}

class ServeHttpStreamSource final : public proxygen::coro::HTTPSource {
public:
  explicit ServeHttpStreamSource(std::shared_ptr<ServeHttpClient> client)
    : client_{std::move(client)} {
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
      auto payload = co_await client_->dequeue();
      if (payload) {
        buffered_ = std::move(*payload);
      } else {
        input_done_ = true;
      }
    }
    if (buffered_.empty()) {
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
    auto guard = folly::makeGuard(lifetime(event));
    co_return event;
  }

  void
  stopReading(folly::Optional<const proxygen::coro::HTTPErrorCode>) override {
    client_->close();
    if (heapAllocated_) {
      delete this;
    }
  }

private:
  std::shared_ptr<ServeHttpClient> client_;
  std::string buffered_;
  bool input_done_ = false;
};

struct ServeHttpServerState {
  std::optional<record> responses;
  std::string stream_path = std::string{default_stream_path};
  std::string stream_method = std::string{default_stream_method};
  std::shared_ptr<ServeHttpClientRegistry> clients;
};

class ServeHttpHandler final : public proxygen::coro::HTTPHandler {
public:
  explicit ServeHttpHandler(std::shared_ptr<ServeHttpServerState> state)
    : state_{std::move(state)} {
  }

  auto handleRequest(folly::EventBase*, proxygen::coro::HTTPSessionContextPtr,
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
    if (state_->responses) {
      if (auto response
          = http::lookup_response(*state_->responses, request_path)) {
        co_return http::make_fixed_response_source(
          response->code, std::move(response->body), response->content_type);
      }
    }
    if (request_path != state_->stream_path) {
      co_return http::make_fixed_response_source(404, std::string{});
    }
    if (not detail::ascii_icase_equal(request_method, state_->stream_method)) {
      co_return http::make_fixed_response_source(405, std::string{});
    }
    auto client = state_->clients->create_client();
    if (not client) {
      co_return http::make_fixed_response_source(503, std::string{});
    }
    co_return proxygen::coro::HTTPSourceHolder{
      new ServeHttpStreamSource{std::move(client)}};
  }

private:
  std::shared_ptr<ServeHttpServerState> state_;
};

struct ServeHttpArgs {
  location op = location::unknown;
  located<secret> url;
  std::optional<located<std::string>> path;
  std::optional<located<std::string>> method;
  std::optional<located<record>> responses;
  std::optional<located<uint64_t>> max_connections;
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
    if (max_connections and max_connections->inner == 0) {
      diagnostic::error("`max_connections` must not be zero")
        .primary(max_connections->source)
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

class ServeHttp final : public Operator<table_slice, void> {
public:
  explicit ServeHttp(ServeHttpArgs args) : args_{std::move(args)} {
  }

  ServeHttp(ServeHttp const&) = delete;
  auto operator=(ServeHttp const&) -> ServeHttp& = delete;
  ServeHttp(ServeHttp&&) noexcept = default;
  auto operator=(ServeHttp&&) noexcept -> ServeHttp& = default;

  ~ServeHttp() override {
    stop_server();
    if (clients_) {
      clients_->shutdown();
    }
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto request = make_secret_request("url", args_.url, resolved_url_, dh);
    auto resolved = co_await ctx.resolve_secrets({std::move(request)});
    if (not resolved) {
      done_ = true;
      co_return;
    }
    if (resolved_url_.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      done_ = true;
      co_return;
    }
    auto endpoint = http::parse_host_port_endpoint(resolved_url_);
    if (endpoint.is_err()) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      done_ = true;
      co_return;
    }
    auto [host, port] = std::move(endpoint).unwrap();
    tls_options_
      = args_.tls
          ? tls_options{*args_.tls, {.tls_default = false, .is_server = true}}
          : tls_options{{.tls_default = false, .is_server = true}};
    auto ssl_result = tls_options_.make_folly_ssl_context(dh);
    if (not ssl_result) {
      done_ = true;
      co_return;
    }
    tls_context_ = std::move(*ssl_result);
    server_state_ = std::make_shared<ServeHttpServerState>();
    server_state_->responses = args_.responses.transform([](auto const& x) {
      return x.inner;
    });
    server_state_->stream_path
      = args_.path ? args_.path->inner : std::string{default_stream_path};
    server_state_->stream_method = std::string{default_stream_method};
    if (args_.method) {
      auto method = http::normalize_http_method(args_.method->inner);
      TENZIR_ASSERT(method);
      server_state_->stream_method = std::move(*method);
    }
    server_state_->clients = clients_;
    auto backlog = default_listen_backlog;
    if (auto max_connections = inner(args_.max_connections)) {
      backlog = detail::narrow<uint32_t>(*max_connections);
    }
    auto* event_base = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(event_base);
    auto socket = folly::AsyncServerSocket::newSocket(event_base);
    auto addr = folly::SocketAddress{host, port, true};
    server_ = Box<folly::coro::ServerSocket>{std::in_place, std::move(socket),
                                             addr, backlog};
    handler_ = std::make_shared<ServeHttpHandler>(server_state_);
    ctx.spawn_task(folly::coro::co_withExecutor(event_base, accept_loop()));
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_ or stopping_) {
      co_return;
    }
    if (input.rows() == 0) {
      co_return;
    }
    for (auto row : input.values()) {
      auto json = to_json(materialize(row), {.oneline = true});
      if (not json) {
        diagnostic::warning("failed to encode event as json")
          .note("skipping event")
          .emit(ctx);
        continue;
      }
      auto line = std::move(*json);
      line.push_back('\n');
      while (true) {
        if (done_ or stopping_ or clients_->is_stopping()) {
          co_return;
        }
        auto clients = clients_->snapshot_clients();
        if (clients.empty()) {
          co_await clients_->wait_for_change();
          continue;
        }
        auto delivered = false;
        for (auto const& client : clients) {
          delivered |= client->enqueue(line);
        }
        if (delivered) {
          break;
        }
        co_await clients_->wait_for_change();
      }
    }
    co_return;
  }

  auto finalize(OpCtx&) -> Task<void> override {
    done_ = true;
    clients_->shutdown();
    stop_server();
    co_return;
  }

  auto stop(OpCtx&) -> Task<void> override {
    stopping_ = true;
    done_ = true;
    clients_->shutdown();
    stop_server();
    co_return;
  }

  auto state() -> OperatorState override {
    if (done_) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

private:
  auto accept_loop() -> Task<void> {
    TENZIR_ASSERT(server_);
    while (not stopping_) {
      std::unique_ptr<folly::coro::Transport> transport;
      try {
        transport = co_await (*server_)->accept();
      } catch (...) {
        if (not stopping_) {
          done_ = true;
          clients_->shutdown();
        }
        co_return;
      }
      if (not transport) {
        continue;
      }
      auto peer_addr = transport->getPeerAddress();
      if (tls_context_) {
        try {
          co_await upgrade_transport_to_tls_server(transport, tls_context_);
        } catch (std::exception const& ex) {
          TENZIR_DEBUG("serve_http: TLS handshake failed with {}: {}",
                       peer_addr.describe(), ex.what());
          continue;
        }
      }
      auto codec = proxygen::HTTPCodecFactory::getCodec(
        proxygen::CodecProtocol::HTTP_1_1,
        proxygen::TransportDirection::DOWNSTREAM);
      TENZIR_ASSERT(codec);
      auto tinfo = wangle::TransportInfo{};
      auto session = proxygen::coro::HTTPCoroSession::makeDownstreamCoroSession(
        std::move(transport), handler_, std::move(codec), std::move(tinfo));
      TENZIR_ASSERT(session);
      session->run().start();
    }
  }

  auto stop_server() -> void {
    stopping_ = true;
    if (server_) {
      (*server_)->close();
    }
  }

  ServeHttpArgs args_;
  std::string resolved_url_;
  std::shared_ptr<ServeHttpServerState> server_state_;
  std::shared_ptr<ServeHttpClientRegistry> clients_
    = std::make_shared<ServeHttpClientRegistry>();
  std::optional<Box<folly::coro::ServerSocket>> server_;
  std::shared_ptr<ServeHttpHandler> handler_;
  std::shared_ptr<folly::SSLContext> tls_context_;
  tls_options tls_options_{{.tls_default = false, .is_server = true}};
  bool stopping_ = false;
  bool done_ = false;
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
    auto max_connections
      = d.named("max_connections", &ServeHttpArgs::max_connections);
    auto tls = d.named("tls", &ServeHttpArgs::tls);
    d.validate([=](ValidateCtx& ctx) -> Empty {
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
      if (auto x = ctx.get(max_connections)) {
        args.max_connections = *x;
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
