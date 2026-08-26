//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "http.hpp"

#include "tenzir/atomic.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/tls_options.hpp"

#include <folly/io/IOBuf.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
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
    co_return http_server::track_response_delivery(
      std::move(response_source),
      [active_requests_limit = active_requests_limit_,
       permit = std::move(*permit)]() mutable {
        TENZIR_UNUSED(active_requests_limit);
        permit.release();
      },
      [queue = queue_]() mutable {
        queue->force_enqueue(HttpResponseDelivered{});
      });
  }

private:
  AcceptOtlpArgs args_;
  Arc<MessageQueue> queue_;
  Arc<Atomic<uint64_t>> request_id_gen_;
  Arc<Semaphore> active_requests_limit_;
};

} // namespace

auto start_http_server(AcceptOtlpArgs const& args, Arc<MessageQueue> queue,
                       Arc<Semaphore> active_requests_limit, OpCtx& ctx)
  -> Task<Option<Box<http_server::Server>>> {
  auto const& cfg = ctx.actor_system().config();
  auto endpoint = std::string{};
  auto requests = std::vector<secret_request>{
    make_secret_request("endpoint", args.endpoint, endpoint, ctx.dh())};
  if ((co_await ctx.resolve_secrets(std::move(requests))).is_error()) {
    co_return None{};
  }
  auto config = http_server::make_config(endpoint, args.endpoint.source,
                                         args.tls, cfg, ctx.dh());
  if (not config) {
    co_return None{};
  }
  auto request_id_gen = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
  auto handler
    = std::make_shared<RequestHandler>(args, std::move(queue),
                                       std::move(request_id_gen),
                                       std::move(active_requests_limit));
  auto server = co_await http_server::Server::start(std::move(*config),
                                                    std::move(handler));
  if (server.is_err()) {
    diagnostic::error("failed to start OTLP/HTTP server: {}",
                      std::move(server).unwrap_err())
      .primary(args.endpoint)
      .emit(ctx);
    co_return None{};
  }
  co_return std::move(server).unwrap();
}

} // namespace tenzir::plugins::accept_otlp::detail
