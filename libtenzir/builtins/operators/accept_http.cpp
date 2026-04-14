//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arc.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/async/oneshot.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/variant.hpp"

#include <folly/ScopeGuard.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/SSLContext.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <proxygen/lib/http/coro/server/ScopedHTTPServer.h>
#include <proxygen/lib/utils/URL.h>

#include <charconv>
#include <cstddef>
#include <limits>
#include <utility>

namespace tenzir::plugins::accept_http {

namespace {

struct AcceptHttpArgs {
  located<secret> endpoint;
  Option<ast::field_path> metadata_field;
  Option<located<data>> responses;
  Option<located<uint64_t>> max_request_size;
  Option<located<uint64_t>> max_connections;
  Option<located<data>> tls;
  located<ir::pipeline> parser;
};

struct ParsedEndpoint {
  std::string host;
  uint16_t port;
  Option<bool> scheme_tls;
};

auto tls_enabled_from_args(AcceptHttpArgs const& args) -> bool {
  if (not args.tls) {
    return false;
  }
  auto tls_opts
    = tls_options{*args.tls, {.tls_default = false, .is_server = true}};
  return tls_opts.get_tls(nullptr).inner;
}

auto parse_folly_tls_version(std::string_view input)
  -> Option<folly::SSLContext::SSLVersion> {
  if (input == "" or input == "any") {
    return folly::SSLContext::SSLVersion::TLSv1;
  }
  if (input == "tls1" or input == "tls1.0" or input == "tlsv1"
      or input == "tlsv1.0") {
    return folly::SSLContext::SSLVersion::TLSv1;
  }
  if (input == "tls1.1" or input == "tlsv1.1") {
    return None{};
  }
  if (input == "tls1.2" or input == "tlsv1.2") {
    return folly::SSLContext::SSLVersion::TLSv1_2;
  }
  if (input == "tls1.3" or input == "tlsv1.3") {
    return folly::SSLContext::SSLVersion::TLSv1_3;
  }
  return None{};
}

auto make_tls_config(AcceptHttpArgs const& args, diagnostic_handler& dh)
  -> failure_or<wangle::SSLContextConfig> {
  auto tls_opts
    = args.tls
        ? tls_options{*args.tls, {.tls_default = false, .is_server = true}}
        : tls_options{{.tls_default = false, .is_server = true}};
  auto certfile = tls_opts.get_certfile(nullptr);
  if (not certfile) {
    diagnostic::error("`tls.certfile` is required when TLS is enabled")
      .primary(args.endpoint)
      .emit(dh);
    return failure::promise();
  }
  auto keyfile = tls_opts.get_keyfile(nullptr);
  auto password = tls_opts.get_password(nullptr);
  auto config = proxygen::coro::HTTPServer::getDefaultTLSConfig();
  if (auto min = tls_opts.get_tls_min_version(nullptr)) {
    if (not min->inner.empty()) {
      if (auto parsed = parse_folly_tls_version(min->inner)) {
        config.sslVersion = *parsed;
      } else {
        diagnostic::error("invalid TLS minimum version: `{}`", min->inner)
          .primary(*min)
          .hint("supported values are `tls1`, `tls1.2`, and `tls1.3`")
          .emit(dh);
        return failure::promise();
      }
    }
  }
  try {
    config.setCertificate(certfile->inner,
                          keyfile ? keyfile->inner : certfile->inner,
                          password ? password->inner : "");
  } catch (std::exception const& ex) {
    diagnostic::error("failed to load TLS certificate: {}", ex.what())
      .primary(*certfile)
      .emit(dh);
    return failure::promise();
  }
  auto require_client_cert
    = tls_opts.get_tls_require_client_cert(nullptr).inner;
  auto skip_peer_verification
    = tls_opts.get_skip_peer_verification(nullptr).inner;
  if (require_client_cert) {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::ALWAYS;
  } else if (skip_peer_verification) {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::DO_NOT_REQUEST;
  } else {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::IF_PRESENTED;
  }
  if (auto client_ca = tls_opts.get_tls_client_ca(nullptr)) {
    config.clientCAFiles.push_back(client_ca->inner);
  }
  if (auto cacert = tls_opts.get_cacert(nullptr)) {
    config.clientCAFiles.push_back(cacert->inner);
  }
  return config;
}

template <class T>
auto parse_number(std::string_view text) -> Option<T> {
  if (text.empty()) {
    return None{};
  }
  auto value = T{};
  auto const* begin = text.data();
  auto const* end = begin + text.size();
  auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} or ptr != end) {
    return None{};
  }
  return value;
}

auto parse_endpoint(std::string_view endpoint, location loc,
                    diagnostic_handler& dh) -> Option<ParsedEndpoint> {
  if (endpoint.contains("://")) {
    auto parsed = proxygen::URL{std::string{endpoint}};
    if (not parsed.isValid() or not parsed.hasHost()) {
      diagnostic::error("failed to parse endpoint URL").primary(loc).emit(dh);
      return None{};
    }
    auto scheme = parsed.getScheme();
    auto scheme_tls = Option<bool>{None{}};
    if (scheme == "https") {
      scheme_tls = true;
    } else if (scheme == "http") {
      scheme_tls = false;
    } else {
      diagnostic::error("unsupported endpoint URL scheme: `{}`", scheme)
        .primary(loc)
        .hint("use `http://` or `https://`")
        .emit(dh);
      return None{};
    }
    return ParsedEndpoint{
      .host = parsed.getHost(),
      .port = parsed.getPort(),
      .scheme_tls = scheme_tls,
    };
  }
  if (endpoint.empty()) {
    diagnostic::error("`endpoint` must not be empty").primary(loc).emit(dh);
    return None{};
  }
  if (endpoint.front() == '[') {
    auto const close = endpoint.find(']');
    if (close == std::string_view::npos) {
      diagnostic::error("invalid IPv6 endpoint syntax")
        .primary(loc)
        .hint("expected `[host]:port`")
        .emit(dh);
      return None{};
    }
    auto const host = endpoint.substr(1, close - 1);
    auto const rest = endpoint.substr(close + 1);
    if (rest.empty() or rest.front() != ':') {
      diagnostic::error("invalid IPv6 endpoint syntax")
        .primary(loc)
        .hint("expected `[host]:port`")
        .emit(dh);
      return None{};
    }
    auto port = parse_number<uint16_t>(rest.substr(1));
    if (not port) {
      diagnostic::error("failed to parse endpoint port").primary(loc).emit(dh);
      return None{};
    }
    return ParsedEndpoint{
      .host = std::string{host},
      .port = *port,
      .scheme_tls = None{},
    };
  }
  auto const colon = endpoint.rfind(':');
  if (colon == std::string_view::npos) {
    diagnostic::error("failed to parse endpoint")
      .primary(loc)
      .hint("expected `host:port`, `[host]:port`, or URL")
      .emit(dh);
    return None{};
  }
  auto const host = endpoint.substr(0, colon);
  auto port = parse_number<uint16_t>(endpoint.substr(colon + 1));
  if (not port) {
    diagnostic::error("failed to parse endpoint port").primary(loc).emit(dh);
    return None{};
  }
  return ParsedEndpoint{
    .host = std::string{host},
    .port = *port,
    .scheme_tls = None{},
  };
}

struct RequestMetadata {
  std::vector<std::pair<std::string, std::string>> headers;
  std::vector<std::pair<std::string, std::string>> query;
  std::string path;
  std::string fragment;
  std::string method;
  std::string version;
};

struct Response {
  uint16_t status = 200;
  std::string content_type;
  std::string body;
};

using ResponseSignal = Oneshot<uint16_t>;

struct RequestStarted {
  uint64_t request_id;
  RequestMetadata metadata;
  std::string content_encoding;
  Arc<ResponseSignal> response_signal;
};

struct RequestBody {
  uint64_t request_id;
  chunk_ptr chunk;
};

struct RequestFinished {
  uint64_t request_id;
};

// needed to make Message default-constructable
struct Noop {};

using Message = variant<Noop, RequestStarted, RequestBody, RequestFinished>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

auto make_request_metadata(proxygen::HTTPMessage const& request)
  -> RequestMetadata {
  auto result = RequestMetadata{};
  request.getHeaders().forEach([&](std::string& k, std::string& v) {
    result.headers.emplace_back(k, v);
  });
  for (auto const& [k, v] : request.getQueryParams()) {
    result.query.emplace_back(k, v);
  }
  result.path = std::string{request.getPathAsStringPiece()};
  auto const& url = request.getURL();
  if (auto hash = url.find('#'); hash != std::string::npos) {
    result.fragment = url.substr(hash + 1);
  }
  result.method = request.getMethodString();
  for (auto& c : result.method) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  auto const [major, minor] = request.getHTTPVersion();
  result.version = std::to_string(static_cast<int>(major)) + "."
                   + std::to_string(static_cast<int>(minor));
  return result;
}

auto get_response_for_path(AcceptHttpArgs const& args, std::string_view path)
  -> Response {
  auto res = Response{};
  if (not args.responses) {
    return res;
  }
  auto const* responses = try_as<record>(args.responses->inner);
  if (not responses) {
    return res;
  }
  auto it = responses->find(std::string{path});
  if (it == responses->end()) {
    return res;
  }
  auto const* entry = try_as<record>(it->second);
  if (not entry) {
    return res;
  }
  auto code_it = entry->find("code");
  auto content_type_it = entry->find("content_type");
  auto body_it = entry->find("body");
  if (code_it == entry->end() or content_type_it == entry->end()
      or body_it == entry->end()) {
    return res;
  }
  auto const* code_i64 = try_as<int64_t>(code_it->second);
  auto const* content_type = try_as<std::string>(content_type_it->second);
  auto const* body = try_as<std::string>(body_it->second);
  if (not code_i64 or not content_type or not body) {
    return res;
  }
  res.status = detail::narrow<uint16_t>(*code_i64);
  res.content_type = *content_type;
  res.body = *body;
  return res;
}

auto make_fixed_response(Response const& response)
  -> proxygen::coro::HTTPSourceHolder {
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedResponse(
    response.status, std::string{response.body});
  if (not response.content_type.empty()) {
    source->msg_->getHeaders().set(proxygen::HTTP_HEADER_CONTENT_TYPE,
                                   response.content_type);
  }
  return source;
}

class RequestHandler final : public proxygen::coro::HTTPHandler {
public:
  RequestHandler(AcceptHttpArgs args, Arc<MessageQueue> queue,
                 Arc<Atomic<uint64_t>> request_id_gen,
                 Arc<Atomic<uint64_t>> active_connections,
                 size_t max_request_size, uint64_t max_connections)
    : args_{std::move(args)},
      queue_{std::move(queue)},
      request_id_gen_{std::move(request_id_gen)},
      active_connections_{std::move(active_connections)},
      max_request_size_{max_request_size},
      max_connections_{max_connections} {
  }

  auto handleRequest(folly::EventBase*, proxygen::coro::HTTPSessionContextPtr,
                     proxygen::coro::HTTPSourceHolder request_source)
    -> folly::coro::Task<proxygen::coro::HTTPSourceHolder> override {
    auto previous
      = active_connections_->fetch_add(1, std::memory_order_acq_rel);
    if (previous >= max_connections_) {
      active_connections_->fetch_sub(1, std::memory_order_acq_rel);
      co_return proxygen::coro::HTTPFixedSource::makeFixedResponse(503);
    }
    auto connection_guard
      = folly::makeGuard([active = active_connections_]() mutable {
          active->fetch_sub(1, std::memory_order_acq_rel);
        });
    auto request_id = request_id_gen_->fetch_add(1, std::memory_order_relaxed);
    std::string path;
    auto bytes_received = size_t{};
    Arc<ResponseSignal> response_signal{std::in_place};
    auto started = false;
    auto reader = proxygen::coro::HTTPSourceReader{std::move(request_source)};
    reader
      .onHeadersAsync([&](std::unique_ptr<proxygen::HTTPMessage> msg,
                          bool is_final, bool) -> folly::coro::Task<bool> {
        if (not is_final) {
          // Ignore informational 1xx headers and wait for final headers.
          co_return proxygen::coro::HTTPSourceReader::Continue;
        }
        TENZIR_ASSERT(not started);
        started = true;
        auto metadata = make_request_metadata(*msg);
        path = metadata.path;
        auto content_length_header = std::string_view{
          msg->getHeaders().getSingleOrEmpty("Content-Length")};
        if (auto content_length = parse_number<size_t>(content_length_header);
            content_length and *content_length > max_request_size_) {
          response_signal->send(413); // payload too large
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }
        auto encoding
          = std::string{msg->getHeaders().getSingleOrEmpty("Content-Encoding")};
        co_await queue_->enqueue(
          RequestStarted{.request_id = request_id,
                         .metadata = std::move(metadata),
                         .content_encoding = std::move(encoding),
                         .response_signal = response_signal});
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onBodyAsync([&](quic::BufQueue queue, bool) -> folly::coro::Task<bool> {
        TENZIR_ASSERT(started);
        if (response_signal->has_sent()) {
          co_return proxygen::coro::HTTPSourceReader::Cancel;
        }

        if (not queue.empty()) {
          auto iobuf = queue.move();
          iobuf->coalesce();
          if (bytes_received + iobuf->length() > max_request_size_) {
            response_signal->send(413); // payload too large
            co_return proxygen::coro::HTTPSourceReader::Cancel;
          }
          bytes_received += iobuf->length();
          auto payload = chunk::copy(
            std::span{reinterpret_cast<std::byte const*>(iobuf->data()),
                      iobuf->length()});
          co_await queue_->enqueue(
            RequestBody{.request_id = request_id, .chunk = std::move(payload)});
        }
        co_return proxygen::coro::HTTPSourceReader::Continue;
      })
      .onError([&](proxygen::coro::HTTPSourceReader::ErrorContext,
                   const proxygen::coro::HTTPError&) {
        response_signal->send(400);
      });
    // read request
    co_await reader.read(detail::narrow<uint32_t>(max_request_size_));
    // notify request finished
    co_await queue_->enqueue(RequestFinished{request_id});
    // send response
    auto res_status = co_await response_signal->recv();
    if (res_status != 200) {
      co_return proxygen::coro::HTTPFixedSource::makeFixedResponse(res_status);
    }
    auto response = get_response_for_path(args_, path);
    co_return make_fixed_response(response);
  }

private:
  AcceptHttpArgs args_;
  Arc<MessageQueue> queue_;
  Arc<Atomic<uint64_t>> request_id_gen_;
  Arc<Atomic<uint64_t>> active_connections_;
  size_t max_request_size_;
  uint64_t max_connections_;
};

class AcceptHttp final : public Operator<void, table_slice> {
public:
  explicit AcceptHttp(AcceptHttpArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto config = co_await make_config(ctx);
    if (not config) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    auto request_id_gen = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
    auto active_connections = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
    auto request_handler = std::make_shared<RequestHandler>(
      args_, message_queue_, request_id_gen, active_connections,
      get_max_request_size(), get_max_connections());
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
    // HACK: if this operator gets forcefully stopped via task cancellation
    // this task will catch cancellation and tell the server to respond to
    // requests that have already finished. Other requests will get
    // disconnected, but I guess this is fine.
    ctx.spawn_task([this]() -> Task<void> {
      co_await catch_cancellation(wait_forever());
      auto active_requests = co_await active_requests_.lock();
      for (auto& it : *active_requests) {
        it.second.finished->send(200); // ok
      }
    });
    lifecycle_ = Lifecycle::running;
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto message = std::move(result).as<Message>();
    co_await co_match(
      std::move(message),
      [&](RequestStarted msg) -> Task<void> {
        if (lifecycle_ != Lifecycle::running) {
          msg.response_signal->send(503); // service unavailable
          co_return;
        }

        auto pipeline = args_.parser.inner;
        if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
          diagnostic::warning("failed to prepare parser pipeline for request")
            .primary(args_.endpoint)
            .note("request path: {}", msg.metadata.path)
            .emit(ctx);
          msg.response_signal->send(500); // internal server error
          co_return;
        }
        auto decompressor
          = Option<std::shared_ptr<arrow::util::Decompressor>>{None{}};
        if (not msg.content_encoding.empty()) {
          decompressor = http::make_decompressor(msg.content_encoding, ctx);
        }
        auto request_id = msg.request_id;
        {
          auto active_requests = co_await active_requests_.lock();
          active_requests->emplace(
            request_id, ActiveRequest{.metadata = std::move(msg.metadata),
                                      .decompressor = std::move(decompressor),
                                      .finished = msg.response_signal});
        }
        co_await ctx.spawn_sub(request_id, std::move(pipeline),
                               tag_v<chunk_ptr>);
      },
      [&](RequestBody body) -> Task<void> {
        auto chunk = std::move(body.chunk);
        {
          auto active_requests = co_await active_requests_.lock();
          auto it = active_requests->find(body.request_id);
          if (it == active_requests->end()) {
            // Request was dropped (e.g. pipeline substitution failed).
            co_return;
          }
          auto& req = it->second;
          if (req.drop_body) {
            co_return;
          }
          if (req.decompressor) {
            auto remaining = get_max_request_size() - req.output_bytes;
            auto input_span = std::span<std::byte const>{
              reinterpret_cast<std::byte const*>(chunk->data()), chunk->size()};
            auto decompressed = http::decompress_chunk(
              **req.decompressor, input_span, ctx.dh(), remaining);
            if (not decompressed) {
              req.drop_body = true;
              co_return;
            }
            chunk = chunk::make(std::move(*decompressed));
          }
          if (req.output_bytes + chunk->size() > get_max_request_size()) {
            diagnostic::warning("request body exceeds `max_request_size`")
              .primary(args_.endpoint)
              .note("request path: {}", req.metadata.path)
              .note("rejecting request body")
              .emit(ctx);
            req.drop_body = true;
            req.finished->send(413); // payload too large
            co_return;
          }
          req.output_bytes += chunk->size();
        }
        if (auto sub = ctx.get_sub(body.request_id)) {
          auto& parser = as<SubHandle<chunk_ptr>>(*sub);
          auto push_result = co_await parser.push(std::move(chunk));
          if (push_result.is_err()) {
            auto active_requests = co_await active_requests_.lock();
            if (auto it = active_requests->find(body.request_id);
                it != active_requests->end()) {
              it->second.drop_body = true;
            }
          }
        }
      },
      [&](RequestFinished msg) -> Task<void> {
        if (auto sub = ctx.get_sub(msg.request_id)) {
          auto& parser = as<SubHandle<chunk_ptr>>(*sub);
          co_await parser.close();
        }
      },
      [&](Noop) -> Task<void> {
        co_return;
      });
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    if (args_.metadata_field) {
      auto request_id = as<uint64_t>(key);
      slice = co_await attach_metadata(request_id, std::move(slice), ctx.dh());
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto request_id = as<uint64_t>(key);
    auto should_finish = false;
    {
      auto active_requests = co_await active_requests_.lock();
      auto it = active_requests->find(request_id);
      if (it == active_requests->end()) {
        co_return;
      }
      // notify the handler to respond
      it->second.finished->send(200);
      active_requests->erase(request_id);
      should_finish
        = lifecycle_ == Lifecycle::draining and active_requests->empty();
    }
    if (should_finish) {
      server_ = None{};
      lifecycle_ = Lifecycle::done;
    }
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    // finalize will only be called when state() returns done
    TENZIR_ASSERT(lifecycle_ == Lifecycle::done);
    co_return FinalizeBehavior::done;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await request_stop();
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  struct ActiveRequest {
    RequestMetadata metadata;
    // Non-null only when the request carries a supported Content-Encoding.
    Option<std::shared_ptr<arrow::util::Decompressor>> decompressor;
    size_t output_bytes = 0;
    bool drop_body = false;
    Arc<ResponseSignal> finished;
  };

  auto attach_metadata(uint64_t request_id, table_slice slice,
                       diagnostic_handler& dh) -> Task<table_slice> {
    TENZIR_ASSERT(args_.metadata_field);
    auto metadata = Option<RequestMetadata>{None{}};
    {
      auto active_requests = co_await active_requests_.lock();
      auto it = active_requests->find(request_id);
      if (it == active_requests->end()) {
        co_return slice;
      }
      metadata = it->second.metadata;
    }
    TENZIR_ASSERT(metadata);
    auto sb = series_builder{};
    for (auto i = uint64_t{}; i < slice.rows(); ++i) {
      auto rb = sb.record();
      auto hb = rb.field("headers").record();
      for (auto const& [k, v] : metadata->headers) {
        hb.field(k, v);
      }
      auto qb = rb.field("query").record();
      for (auto const& [k, v] : metadata->query) {
        qb.field(k, v);
      }
      rb.field("path", metadata->path);
      rb.field("fragment", metadata->fragment);
      rb.field("method", metadata->method);
      rb.field("version", metadata->version);
    }
    co_return assign(*args_.metadata_field, sb.finish_assert_one_array(), slice,
                     dh);
  }

  auto request_stop() -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    lifecycle_ = Lifecycle::draining;
    // stop listening for new connections
    if (server_) {
      (*server_)->getServer().drain();
    }
    auto active_requests = co_await active_requests_.lock();
    if (active_requests->empty()) {
      server_ = None{};
      lifecycle_ = Lifecycle::done;
    }
    co_return;
  }

  auto make_config(OpCtx& ctx)
    -> Task<Option<proxygen::coro::HTTPServer::Config>> {
    auto resolved_endpoint = std::string{};
    auto requests = std::vector<secret_request>{};
    requests.emplace_back(make_secret_request("endpoint", args_.endpoint,
                                              resolved_endpoint, ctx.dh()));
    if (auto result = co_await ctx.resolve_secrets(std::move(requests));
        result.is_error()) {
      co_return None{};
    }
    auto parsed
      = parse_endpoint(resolved_endpoint, args_.endpoint.source, ctx.dh());
    if (not parsed) {
      co_return None{};
    }
    auto tls_enabled = tls_enabled_from_args(args_);
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
    config.socketConfig.maxNumPendingConnectionsPerWorker
      = detail::narrow<uint32_t>(get_max_connections());
    if (tls_enabled) {
      auto tls_config = make_tls_config(args_, ctx.dh());
      if (not tls_config) {
        co_return None{};
      }
      config.socketConfig.sslContextConfigs.emplace_back(
        std::move(*tls_config));
    }
    co_return config;
  }

  auto get_max_request_size() const -> size_t {
    if (not args_.max_request_size) {
      return static_cast<size_t>(10 * 1024 * 1024);
    }
    return detail::narrow<size_t>(args_.max_request_size->inner);
  }

  auto get_max_connections() const -> uint64_t {
    return args_.max_connections ? args_.max_connections->inner : uint64_t{10};
  }

  // --- config ---
  AcceptHttpArgs args_;
  // --- transient ---
  mutable Arc<MessageQueue> message_queue_{std::in_place, uint32_t{256}};
  Option<Arc<proxygen::coro::ScopedHTTPServer>> server_;
  Mutex<std::unordered_map<uint64_t, ActiveRequest>> active_requests_{{}};
  // --- state ---
  Lifecycle lifecycle_ = Lifecycle::running;
};

class AcceptHttpPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "accept_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<AcceptHttpArgs, AcceptHttp>{};
    d.positional("endpoint", &AcceptHttpArgs::endpoint);
    d.named("metadata_field", &AcceptHttpArgs::metadata_field);
    auto responses_arg
      = d.named("responses", &AcceptHttpArgs::responses, "record");
    auto max_request_size_arg
      = d.named("max_request_size", &AcceptHttpArgs::max_request_size);
    auto max_connections_arg
      = d.named("max_connections", &AcceptHttpArgs::max_connections);
    auto tls_validator
      = tls_options{{.tls_default = false, .is_server = true}}.add_to_describer(
        d, &AcceptHttpArgs::tls);
    auto parser_arg = d.pipeline(&AcceptHttpArgs::parser);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      if (auto responses = ctx.get(responses_arg)) {
        auto const* rec = try_as<record>(responses->inner);
        if (not rec) {
          diagnostic::error("`responses` must be a record")
            .primary(responses->source)
            .emit(ctx);
        } else if (rec->empty()) {
          diagnostic::error("`responses` must not be empty")
            .primary(responses->source)
            .emit(ctx);
        } else {
          for (auto const& [path, value] : *rec) {
            auto const* entry = try_as<record>(value);
            if (not entry) {
              diagnostic::error("`responses.{}` must be a record", path)
                .primary(responses->source)
                .emit(ctx);
              continue;
            }
            auto code_it = entry->find("code");
            if (code_it == entry->end() or not is<int64_t>(code_it->second)) {
              diagnostic::error("`responses.{}.code` must be `int`", path)
                .primary(responses->source)
                .emit(ctx);
              continue;
            }
            auto content_type_it = entry->find("content_type");
            if (content_type_it == entry->end()
                or not is<std::string>(content_type_it->second)) {
              diagnostic::error("`responses.{}.content_type` must be `string`",
                                path)
                .primary(responses->source)
                .emit(ctx);
              continue;
            }
            auto body_it = entry->find("body");
            if (body_it == entry->end()
                or not is<std::string>(body_it->second)) {
              diagnostic::error("`responses.{}.body` must be `string`", path)
                .primary(responses->source)
                .emit(ctx);
              continue;
            }
            auto const* code_i64 = try_as<int64_t>(code_it->second);
            if (not code_i64 or *code_i64 < 100 or *code_i64 >= 600) {
              diagnostic::error("got invalid http status code `{}`", *code_i64)
                .primary(responses->source)
                .emit(ctx);
              continue;
            }
          }
        }
      }
      if (auto max_request_size = ctx.get(max_request_size_arg)) {
        if (max_request_size->inner == 0) {
          diagnostic::error("`max_request_size` must be greater than 0")
            .primary(max_request_size->source)
            .emit(ctx);
        } else if (max_request_size->inner > static_cast<uint64_t>(
                     std::numeric_limits<uint32_t>::max())) {
          diagnostic::error("`max_request_size` is too large")
            .primary(max_request_size->source)
            .note("maximum supported value: {}",
                  std::numeric_limits<uint32_t>::max())
            .emit(ctx);
        }
      }
      if (auto max_connections = ctx.get(max_connections_arg)) {
        if (max_connections->inner == 0) {
          diagnostic::error("`max_connections` must be greater than 0")
            .primary(max_connections->source)
            .emit(ctx);
        } else if (max_connections->inner > static_cast<uint64_t>(
                     std::numeric_limits<uint32_t>::max())) {
          diagnostic::error("`max_connections` is too large")
            .primary(max_connections->source)
            .note("maximum supported value: {}",
                  std::numeric_limits<uint32_t>::max())
            .emit(ctx);
        }
      }
      TRY(auto parser, ctx.get(parser_arg));
      auto output = parser.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(parser.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::accept_http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::accept_http::AcceptHttpPlugin)
