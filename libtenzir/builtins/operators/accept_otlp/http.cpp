//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "http.hpp"

#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/tls_options.hpp"

#include <folly/io/IOBuf.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/coro/HTTPByteEvents.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceFilter.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>

#include <cctype>
#include <memory>
#include <thread>
#include <utility>

namespace tenzir::plugins::accept_otlp::detail {

auto make_error_response(uint16_t status) -> HttpResponse {
  return {.status = status, .content_type = {}, .body = {}};
}

auto make_success_response(Encoding encoding) -> HttpResponse {
  if (encoding == Encoding::json) {
    return {.status = 200, .content_type = "application/json", .body = "{}"};
  }
  return {.status = 200, .content_type = "application/x-protobuf", .body = {}};
}

namespace {

auto trim_media_type(std::string value) -> std::string {
  if (auto semicolon = value.find(';'); semicolon != std::string::npos) {
    value.resize(semicolon);
  }
  while (not value.empty()
         and std::isspace(static_cast<unsigned char>(value.back()))) {
    value.pop_back();
  }
  auto first = std::ranges::find_if_not(value, [](unsigned char c) {
    return std::isspace(c);
  });
  value.erase(value.begin(), first);
  return tenzir::detail::ascii_tolower(value);
}

auto classify_path(std::string_view path) -> Option<Signal> {
  if (path == "/v1/logs") {
    return Signal::logs;
  }
  if (path == "/v1/metrics") {
    return Signal::metrics;
  }
  if (path == "/v1/traces") {
    return Signal::traces;
  }
  return None{};
}

auto classify_content_type(std::string value) -> Option<Encoding> {
  value = trim_media_type(std::move(value));
  if (value == "application/x-protobuf") {
    return Encoding::protobuf;
  }
  if (value == "application/json") {
    return Encoding::json;
  }
  return None{};
}

auto parse_request_headers(proxygen::HTTPMessage const& request,
                           std::string const& peer_ip,
                           AcceptOtlpArgs const& args)
  -> Result<std::pair<RequestMetadata, bool>, HttpResponse> {
  if (request.getMethodString() != "POST") {
    return Err{make_error_response(405)};
  }
  auto signal = classify_path(request.getPathAsStringPiece());
  if (not signal or not args.accepts(*signal)) {
    return Err{make_error_response(404)};
  }
  auto encoding = classify_content_type(
    std::string{request.getHeaders().getSingleOrEmpty("Content-Type")});
  if (not encoding) {
    return Err{make_error_response(415)};
  }
  auto content_encoding = tenzir::detail::ascii_tolower(
    request.getHeaders().getSingleOrEmpty("Content-Encoding"));
  if (not content_encoding.empty() and content_encoding != "gzip") {
    return Err{make_error_response(415)};
  }
  auto metadata = RequestMetadata{
    .client_ip = peer_ip,
    .metadata = {},
    .signal = *signal,
    .encoding = *encoding,
    .transport = Transport::http,
  };
  request.getHeaders().forEach(
    [&](std::string const& key, std::string const& value) {
      metadata.metadata.emplace_back(tenzir::detail::ascii_tolower(key), value);
    });
  return std::pair{std::move(metadata), content_encoding == "gzip"};
}

class ResponseCompletion final : public proxygen::coro::HTTPByteEventCallback {
public:
  ResponseCompletion(Arc<MessageQueue> queue,
                     Arc<Semaphore> active_requests_limit,
                     SemaphorePermit permit)
    : queue_{std::move(queue)},
      active_requests_limit_{std::move(active_requests_limit)},
      permit_{std::move(permit)} {
  }

  auto onByteEvent(proxygen::coro::HTTPByteEvent event) -> void override {
    switch (event.type) {
      case proxygen::coro::HTTPByteEvent::Type::KERNEL_WRITE:
        kernel_write_finished_ = true;
        release_admission();
        if (ack_finished_) {
          finish();
        }
        break;
      case proxygen::coro::HTTPByteEvent::Type::CUMULATIVE_ACK:
        release_admission();
        finish();
        break;
      case proxygen::coro::HTTPByteEvent::Type::TRANSPORT_WRITE:
      case proxygen::coro::HTTPByteEvent::Type::NIC_TX:
        break;
    }
  }

  auto onByteEventCanceled(proxygen::coro::HTTPByteEvent event,
                           proxygen::coro::HTTPError) -> void override {
    switch (event.type) {
      case proxygen::coro::HTTPByteEvent::Type::KERNEL_WRITE:
        release_admission();
        finish();
        break;
      case proxygen::coro::HTTPByteEvent::Type::CUMULATIVE_ACK:
        ack_finished_ = true;
        if (kernel_write_finished_) {
          finish();
        }
        break;
      case proxygen::coro::HTTPByteEvent::Type::TRANSPORT_WRITE:
      case proxygen::coro::HTTPByteEvent::Type::NIC_TX:
        break;
    }
  }

  auto finish() -> void {
    if (finished_) {
      return;
    }
    finished_ = true;
    release_admission();
    queue_->force_enqueue(HttpResponseDelivered{});
    if (numWeakRefCountedPtrs() == 0) {
      delete this;
    }
  }

private:
  auto release_admission() -> void {
    permit_.release();
  }

  auto onWeakRefCountedPtrDestroy() -> void override {
    if (finished_ and numWeakRefCountedPtrs() == 0) {
      delete this;
    }
  }

  Arc<MessageQueue> queue_;
  // The permit refers to this semaphore, so keep it alive until completion.
  Arc<Semaphore> active_requests_limit_;
  SemaphorePermit permit_;
  bool kernel_write_finished_ = false;
  bool ack_finished_ = false;
  bool finished_ = false;
};

class DeliveryTrackingSource final : public proxygen::coro::HTTPSourceFilter {
public:
  DeliveryTrackingSource(proxygen::coro::HTTPSourceHolder source,
                         Arc<MessageQueue> queue,
                         Arc<Semaphore> active_requests_limit,
                         SemaphorePermit permit)
    : HTTPSourceFilter{source.release()},
      completion_{new ResponseCompletion{std::move(queue),
                                         std::move(active_requests_limit),
                                         std::move(permit)}} {
    setHeapAllocated();
  }

  ~DeliveryTrackingSource() override {
    finish_without_delivery_event();
  }

  auto readHeaderEvent()
    -> folly::coro::Task<proxygen::coro::HTTPHeaderEvent> override {
    auto event = co_await folly::coro::co_awaitTry(readHeaderEventImpl());
    auto guard = folly::makeGuard(lifetime(event));
    if (event.hasException()) {
      finish_without_delivery_event();
      co_yield folly::coro::co_error(proxygen::coro::getHTTPError(event));
    }
    if (event->eom) {
      track_delivery(event->byteEventRegistrations);
    }
    co_return std::move(*event);
  }

  auto readBodyEvent(uint32_t max)
    -> folly::coro::Task<proxygen::coro::HTTPBodyEvent> override {
    auto event = co_await folly::coro::co_awaitTry(readBodyEventImpl(max));
    auto guard = folly::makeGuard(lifetime(event));
    if (event.hasException()) {
      finish_without_delivery_event();
      co_yield folly::coro::co_error(proxygen::coro::getHTTPError(event));
    }
    if (event->eom) {
      track_delivery(event->byteEventRegistrations);
    }
    co_return std::move(*event);
  }

  auto stopReading(folly::Optional<const proxygen::coro::HTTPErrorCode> error
                   = folly::none) noexcept -> void override {
    finish_without_delivery_event();
    HTTPSourceFilter::stopReading(error);
  }

private:
  auto track_delivery(
    std::vector<proxygen::coro::HTTPByteEventRegistration>& registrations)
    -> void {
    TENZIR_ASSERT(completion_);
    auto registration = proxygen::coro::HTTPByteEventRegistration{};
    registration.events
      = static_cast<uint8_t>(proxygen::coro::HTTPByteEvent::Type::KERNEL_WRITE)
        | static_cast<uint8_t>(
          proxygen::coro::HTTPByteEvent::Type::CUMULATIVE_ACK);
    registration.callback = completion_->getWeakRefCountedPtr();
    registrations.emplace_back(std::move(registration));
    completion_ = nullptr;
  }

  auto finish_without_delivery_event() noexcept -> void {
    if (auto* completion = std::exchange(completion_, nullptr)) {
      completion->finish();
    }
  }

  ResponseCompletion* completion_;
};

class RequestHandler final : public proxygen::coro::HTTPHandler {
public:
  RequestHandler(AcceptOtlpArgs args, Arc<MessageQueue> queue,
                 Arc<Atomic<uint64_t>> request_id_gen,
                 Arc<Semaphore> active_requests_limit)
    : args_{std::move(args)},
      queue_{std::move(queue)},
      request_id_gen_{std::move(request_id_gen)},
      active_requests_limit_{std::move(active_requests_limit)} {
  }

  auto handleRequest(folly::EventBase*,
                     proxygen::coro::HTTPSessionContextPtr session,
                     proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    auto permit = active_requests_limit_->try_acquire();
    if (not permit) {
      co_return proxygen::coro::HTTPFixedSource::makeFixedResponse(503);
    }
    auto const request_id
      = request_id_gen_->fetch_add(1, std::memory_order_relaxed);
    auto const peer_ip = session->getPeerAddress().getAddressStr();
    auto compressed_bytes = size_t{};
    auto queued = false;
    auto aborted = false;
    Arc<ResponseSignal> response_signal{std::in_place};
    auto reader = proxygen::coro::HTTPSourceReader{std::move(request_source)};
    reader
      .onHeadersAsync([&](std::unique_ptr<proxygen::HTTPMessage> msg,
                          bool is_final, bool) -> folly::coro::Task<bool> {
        if (not is_final) {
          co_return proxygen::coro::HTTPSourceReader::Continue;
        }
        auto parsed = parse_request_headers(*msg, peer_ip, args_);
        if (parsed.is_err()) {
          response_signal->send(std::move(parsed).unwrap_err());
          aborted = true;
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        auto const content_length = http_server::parse_number<size_t>(
          msg->getHeaders().getSingleOrEmpty("Content-Length"));
        if (content_length and *content_length > args_.get_max_message_size()) {
          response_signal->send(make_error_response(413));
          aborted = true;
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        auto [metadata, gzip] = std::move(parsed).unwrap();
        queued = true;
        co_await queue_->enqueue(
          RequestStarted{.request_id = request_id,
                         .metadata = std::move(metadata),
                         .gzip = gzip,
                         .response_signal = response_signal});
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onBodyAsync([&](quic::BufQueue body, bool) -> folly::coro::Task<bool> {
        if (aborted or response_signal->has_sent()) {
          aborted = true;
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        if (body.empty()) {
          co_return proxygen::coro::HTTPSourceReader::Continue;
        }
        auto iobuf = body.move();
        iobuf->coalesce();
        if (compressed_bytes + iobuf->length() > args_.get_max_message_size()) {
          response_signal->send(make_error_response(413));
          aborted = true;
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        compressed_bytes += iobuf->length();
        auto payload = chunk::copy(std::span{
          reinterpret_cast<std::byte const*>(iobuf->data()), iobuf->length()});
        iobuf.reset();
        co_await queue_->enqueue(
          RequestBody{.request_id = request_id, .chunk = std::move(payload)});
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onError([&](proxygen::coro::HTTPSourceReader::ErrorContext,
                   proxygen::coro::HTTPError const&) {
        aborted = true;
        response_signal->send(make_error_response(400));
      });
    co_await reader.read(
      tenzir::detail::narrow<uint32_t>(args_.get_max_message_size()));
    if (queued) {
      co_await queue_->enqueue(RequestFinished{request_id, aborted});
    }
    auto response = co_await response_signal->recv();
    auto response_source
      = response.content_type.empty()
          ? proxygen::coro::HTTPSourceHolder{proxygen::coro::HTTPFixedSource::
                                               makeFixedResponse(
                                                 response.status)}
          : http_server::make_response(response.status, response.content_type,
                                       std::move(response.body));
    co_await queue_->enqueue(HttpResponsePending{});
    co_return new DeliveryTrackingSource{std::move(response_source), queue_,
                                         active_requests_limit_,
                                         std::move(*permit)};
  }

private:
  AcceptOtlpArgs args_;
  Arc<MessageQueue> queue_;
  Arc<Atomic<uint64_t>> request_id_gen_;
  Arc<Semaphore> active_requests_limit_;
};

} // namespace

OtlpHttpServer::OtlpHttpServer(
  std::unique_ptr<http_server::ScopedServer> server) noexcept
  : server_{std::in_place,
            Box<http_server::ScopedServer>::from_non_null(std::move(server))} {
}

OtlpHttpServer::~OtlpHttpServer() {
  force_stop();
}

auto OtlpHttpServer::drain() -> void {
  (*server_)->server().drain();
}

auto OtlpHttpServer::finish() -> void {
  if (not server_) {
    return;
  }
  std::thread{[server = std::exchange(server_, None{})] {}}.detach();
}

auto OtlpHttpServer::force_stop() -> void {
  if (not server_) {
    return;
  }
  (*server_)->server().forceStop();
  finish();
}

auto start_http_server(AcceptOtlpArgs const& args, Arc<MessageQueue> queue,
                       Arc<Semaphore> active_requests_limit, OpCtx& ctx)
  -> Task<Option<Box<OtlpHttpServer>>> {
  auto const& cfg = ctx.actor_system().config();
  auto endpoint = std::string{};
  auto requests = std::vector<secret_request>{
    make_secret_request("endpoint", args.endpoint, endpoint, ctx.dh())};
  if ((co_await ctx.resolve_secrets(std::move(requests))).is_error()) {
    co_return None{};
  }
  auto parsed
    = http_server::parse_endpoint(endpoint, args.endpoint.source, ctx.dh());
  if (not parsed) {
    co_return None{};
  }
  auto const tls_enabled = http_server::is_tls_enabled(args.tls, cfg);
  if (parsed->scheme_tls) {
    if (*parsed->scheme_tls and not tls_enabled) {
      diagnostic::error("`https://` endpoint requires `tls=true`")
        .primary(args.endpoint)
        .emit(ctx);
      co_return None{};
    }
    if (not *parsed->scheme_tls and tls_enabled) {
      diagnostic::error("`http://` endpoint requires `tls=false`")
        .primary(args.endpoint)
        .emit(ctx);
      co_return None{};
    }
  }
  auto config = proxygen::coro::HTTPServer::Config{};
  try {
    config.socketConfig.bindAddress.setFromHostPort(parsed->host, parsed->port);
  } catch (std::exception const& ex) {
    diagnostic::error("failed to configure OTLP endpoint: {}", ex.what())
      .primary(args.endpoint)
      .emit(ctx);
    co_return None{};
  }
  config.numIOThreads = 1;
  if (tls_enabled) {
    auto options = tls_options::from_optional(args.tls, {.tls_default = false,
                                                         .is_server = true});
    auto tls = options.resolve(cfg, ctx);
    if (not tls) {
      co_return None{};
    }
    auto tls_config
      = http_server::make_ssl_context_config(*tls, args.endpoint.source, ctx);
    if (not tls_config) {
      co_return None{};
    }
    config.socketConfig.sslContextConfigs.emplace_back(std::move(*tls_config));
  }
  config.shutdownOnSignals = {};
  auto request_id_gen = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
  auto handler
    = std::make_shared<RequestHandler>(args, std::move(queue),
                                       std::move(request_id_gen),
                                       std::move(active_requests_limit));
  auto server = co_await spawn_blocking(
    [config = std::move(config), handler = std::move(handler)]() mutable {
      return http_server::ScopedServer::start(std::move(config),
                                              std::move(handler));
    });
  if (server.is_err()) {
    diagnostic::error("failed to start OTLP/HTTP server: {}",
                      std::move(server).unwrap_err())
      .primary(args.endpoint)
      .emit(ctx);
    co_return None{};
  }
  co_return Box<OtlpHttpServer>{std::in_place, std::move(server).unwrap()};
}

} // namespace tenzir::plugins::accept_otlp::detail
