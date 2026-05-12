//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/result.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/try.hpp>
#include <tenzir/variant.hpp>

#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <folly/ScopeGuard.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocketException.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/HTTPError.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/client/CoroDNSResolver.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/http/coro/client/HTTPCoroConnector.h>
#include <proxygen/lib/utils/URL.h>

#include <chrono>
#include <iterator>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::from_http {

namespace {

struct FromHttpArgs {
  located<secret> url;
  Option<located<std::string>> method;
  Option<located<data>> body;
  Option<located<data>> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<ast::field_path> error_field;
  Option<located<data>> metadata_field;
  Option<ast::expression> paginate;
  Option<located<duration>> paginate_delay;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  Option<located<std::string>> encode;
  Option<location> server;
  located<ir::pipeline> parser;
  let_id response;
  location operator_location = location::unknown;
};

// Messages from the fetch task to the operator.
struct ResponseHeader {
  uint16_t status;
  std::vector<std::pair<std::string, std::string>> headers;
};
struct ResponseBody {
  chunk_ptr data;
};
struct FetchError {
  std::string message;
};
struct FetchDone {};
struct RetryWarning {
  std::string message;
};

using Message
  = variant<ResponseHeader, ResponseBody, FetchError, FetchDone, RetryWarning>;
using MessageQueue = folly::coro::BoundedQueue<Message>;

// Builds a Proxygen request source, working around a bug in Proxygen's
// internal makeHTTPRequestSource where IOBuf::takeOwnership arguments are
// evaluated in unspecified order.
auto build_request_source(
  proxygen::URL const& url, proxygen::HTTPMethod method,
  const std::vector<std::pair<std::string, std::string>>& headers,
  std::unique_ptr<folly::IOBuf> body_buf) -> proxygen::coro::HTTPSourceHolder {
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedRequest(
    url.makeRelativeURL(), method, std::move(body_buf));
  for (auto const& [k, v] : headers) {
    source->msg_->getHeaders().add(k, v);
  }
  if (not source->msg_->getHeaders().exists(proxygen::HTTP_HEADER_HOST)) {
    source->msg_->getHeaders().add(proxygen::HTTP_HEADER_HOST,
                                   url.getHostAndPortOmitDefault());
  }
  source->msg_->setWantsKeepalive(true);
  source->msg_->setSecure(url.isSecure());
  source->setHeapAllocated();
  return proxygen::coro::HTTPSourceHolder{source};
}

struct RequestConfig {
  proxygen::HTTPMethod method;
  std::vector<std::pair<std::string, std::string>> headers;
  std::vector<std::byte> body;
};

auto has_header(std::span<const std::pair<std::string, std::string>> headers,
                std::string_view name) -> bool {
  return std::ranges::any_of(headers, [&](auto const& kv) {
    return detail::ascii_icase_equal(kv.first, name);
  });
}

// Builds the initial request configuration from the operator arguments.
// Resolves method, serializes the body (records become JSON, blobs are sent
// verbatim), and assembles request headers.
auto make_request_config(
  FromHttpArgs const& args,
  std::vector<std::pair<std::string, std::string>> headers) -> RequestConfig {
  // Resolve HTTP method (validated at describe time).
  // Default to POST when a body is present, GET otherwise.
  auto method_str = args.method ? args.method->inner
                    : args.body ? "POST"
                                : "GET";
  std::transform(method_str.begin(), method_str.end(), method_str.begin(),
                 ::toupper);
  auto method = proxygen::stringToMethod(method_str);
  TENZIR_ASSERT(method);
  // Serialize the optional body. Records become JSON; blobs and strings are
  // sent verbatim.
  auto body = std::vector<std::byte>{};
  Option<std::string> content_type;
  if (args.body) {
    match(
      args.body->inner,
      [&](std::string const& s) {
        auto const* p = reinterpret_cast<std::byte const*>(s.data());
        body.insert(body.end(), p, p + s.size());
      },
      [&](blob const& b) {
        body.insert(body.end(), b.begin(), b.end());
      },
      [&](record const& r) {
        if (args.encode and args.encode->inner == "form") {
          auto buf = curl::escape(flatten(r));
          auto const* p = reinterpret_cast<std::byte const*>(buf.data());
          body.insert(body.end(), p, p + buf.size());
          content_type = "application/x-www-form-urlencoded";
        } else {
          auto buf = std::string{};
          auto p = json_printer{{}};
          auto it = std::back_inserter(buf);
          p.print(it, r);
          auto const* p2 = reinterpret_cast<std::byte const*>(buf.data());
          body.insert(body.end(), p2, p2 + buf.size());
          content_type = "application/json";
        }
      },
      [](auto const&) {
        // Other data types are rejected at describe() time.
      });
  }
  if (content_type and not has_header(headers, "content-type")) {
    headers.emplace_back("Content-Type", content_type.unwrap());
  }
  if (not has_header(headers, "accept")) {
    headers.emplace_back("Accept", "application/json, */*;q=0.5");
  }
  return RequestConfig{
    .method = *method,
    .headers = std::move(headers),
    .body = std::move(body),
  };
}

// Builds the request configuration for a paginated follow-up request.
// Always uses GET with the same headers as the original but no body.
auto make_paginated_request_config(
  std::vector<std::pair<std::string, std::string>> headers) -> RequestConfig {
  if (not has_header(headers, "accept")) {
    headers.emplace_back("Accept", "application/json, */*;q=0.5");
  }
  return RequestConfig{
    .method = proxygen::HTTPMethod::GET,
    .headers = std::move(headers),
    .body = {},
  };
}

auto resolve_http_secrets(
  OpCtx& ctx, FromHttpArgs const& args, std::string& resolved_url,
  std::vector<std::pair<std::string, std::string>>& resolved_headers)
  -> Task<failure_or<bool>> {
  resolved_url.clear();
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(
    make_secret_request("url", args.url, resolved_url, ctx.dh()));
  auto header_requests = http::make_header_secret_requests(
    args.headers, resolved_headers, ctx.dh());
  requests.insert(requests.end(),
                  std::make_move_iterator(header_requests.begin()),
                  std::make_move_iterator(header_requests.end()));
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return failure::promise();
  }
  if (resolved_url.empty()) {
    diagnostic::error("`url` must not be empty").primary(args.url).emit(ctx);
    co_return failure::promise();
  }
  CO_TRY(auto tls_enabled, http::normalize_url_and_tls(
                             args.tls, resolved_url, args.url.source, ctx,
                             std::addressof(ctx.actor_system().config())));
  co_return tls_enabled;
}

using pagination_spec
  = located<variant<ast::lambda_expr, http::PaginationMode>>;

constexpr auto paginate_hint
  = R"(`paginate` must be `"link"`, `"odata"`, or a lambda)";

auto validate_paginate(Option<ast::expression> const& expr,
                       diagnostic_handler& dh)
  -> failure_or<Option<pagination_spec>> {
  if (not expr) {
    return None{};
  }
  if (auto const* lambda = try_as<ast::lambda_expr>(*expr)) {
    if (not lambda->is_unary()) {
      diagnostic::error("expected unary lambda")
        .primary(*expr)
        .hint("binary lambdas are only supported for `sort(..., cmp=...)`")
        .emit(dh);
      return failure::promise();
    }
    return Option<pagination_spec>{pagination_spec{
      variant<ast::lambda_expr, http::PaginationMode>{*lambda},
      expr->get_location(),
    }};
  }
  TRY(auto value, const_eval(*expr, dh));
  return match(
    value,
    [&](std::string const& mode) -> failure_or<Option<pagination_spec>> {
      auto pagination_mode = http::parse_pagination_mode(mode);
      if (not pagination_mode) {
        diagnostic::error("unsupported pagination mode: `{}`", mode)
          .primary(*expr)
          .hint(paginate_hint)
          .emit(dh);
        return failure::promise();
      }
      return Option<pagination_spec>{pagination_spec{
        variant<ast::lambda_expr, http::PaginationMode>{*pagination_mode},
        expr->get_location(),
      }};
    },
    [&](auto const&) -> failure_or<Option<pagination_spec>> {
      diagnostic::error("expected `paginate` to be `string` or `lambda`")
        .primary(*expr)
        .hint(paginate_hint)
        .emit(dh);
      return failure::promise();
    });
}

enum class NextUrlSource {
  paginate_lambda,
  odata_next_link,
};

auto resolve_next_url(std::string_view next_url, std::string const& base_url,
                      location paginate_loc, diagnostic_handler& dh,
                      NextUrlSource source) -> Option<std::string> {
  auto base = boost::urls::parse_uri_reference(base_url);
  if (not base) {
    diagnostic::warning("failed to parse request URI for pagination: {}",
                        base.error().message())
      .primary(paginate_loc)
      .note("stopping pagination")
      .emit(dh);
    return None{};
  }
  auto ref = boost::urls::parse_uri_reference(next_url);
  if (not ref) {
    switch (source) {
      case NextUrlSource::paginate_lambda:
        diagnostic::warning("invalid next URL from `paginate` lambda: {}",
                            ref.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        break;
      case NextUrlSource::odata_next_link:
        diagnostic::warning("invalid OData `@odata.nextLink` URL: {}",
                            ref.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        break;
    }
    return None{};
  }
  auto resolved = boost::urls::url{};
  if (auto r = boost::urls::resolve(*base, *ref, resolved); not r) {
    switch (source) {
      case NextUrlSource::paginate_lambda:
        diagnostic::warning("failed to resolve next URL from `paginate` "
                            "lambda: {}",
                            r.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        break;
      case NextUrlSource::odata_next_link:
        diagnostic::warning("failed to resolve OData `@odata.nextLink` URL: {}",
                            r.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        break;
    }
    return None{};
  }
  return Option<std::string>{std::string{resolved.buffer()}};
}

auto next_url_from_lambda(Option<pagination_spec> const& paginate,
                          table_slice const& slice, std::string const& base_url,
                          diagnostic_handler& dh) -> Option<std::string> {
  if (not paginate) {
    return None{};
  }
  auto const* lambda = try_as<ast::lambda_expr>(&paginate->inner);
  if (not lambda) {
    return None{};
  }
  if (slice.rows() != 1) {
    diagnostic::warning("cannot paginate over multiple events")
      .primary(*paginate)
      .note("stopping pagination")
      .emit(dh);
    return None{};
  }
  auto ms = eval(*lambda, series{slice}, dh);
  auto value = ms.view3_at(0);
  return match(
    value,
    [](caf::none_t const&) -> Option<std::string> {
      return None{};
    },
    [&](std::string_view url) -> Option<std::string> {
      return resolve_next_url(url, base_url, paginate->source, dh,
                              NextUrlSource::paginate_lambda);
    },
    [&](auto const&) -> Option<std::string> {
      diagnostic::error("expected `paginate` to be `string`, got `{}`",
                        ms.parts().front().type.kind())
        .primary(*paginate)
        .emit(dh);
      return None{};
    });
}

auto builtin_pagination_mode(Option<pagination_spec> const& paginate)
  -> Option<http::PaginationMode> {
  if (not paginate) {
    return None{};
  }
  auto const* mode = try_as<http::PaginationMode>(&paginate->inner);
  if (not mode) {
    return None{};
  }
  return *mode;
}

// ---- Fetch task -----------------------------------------------------------

// Configuration for a single fetch (and its retries).
struct FetchConfig {
  std::chrono::milliseconds request_timeout = http::default_timeout;
  std::chrono::milliseconds connection_timeout
    = http::default_connection_timeout;
  uint32_t max_retry_count = http::default_max_retry_count;
  std::chrono::milliseconds retry_delay = http::default_retry_delay;
  std::shared_ptr<folly::SSLContext> tls_context;
};

auto make_fetch_config(FromHttpArgs const& args) -> FetchConfig {
  auto config = FetchConfig{};
  if (args.timeout) {
    config.request_timeout
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        args.timeout->inner);
  }
  if (args.connection_timeout) {
    config.connection_timeout
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        args.connection_timeout->inner);
  }
  if (args.max_retry_count) {
    config.max_retry_count
      = detail::narrow<uint32_t>(args.max_retry_count->inner);
  }
  if (args.retry_delay) {
    config.retry_delay = std::chrono::duration_cast<std::chrono::milliseconds>(
      args.retry_delay->inner);
  }
  return config;
}

struct PaginationState {
  int64_t page_count = 0;
  std::string current_url;
  Option<std::string> next_url;
};

struct ResponseState {
  uint16_t status;
  std::vector<std::pair<std::string, std::string>> headers;
  Option<std::string> content_encoding;
  std::shared_ptr<arrow::util::Decompressor> decompressor;
  blob error_body;

  auto is_success() const -> bool {
    return status < 400;
  }
};

auto make_response_context(ResponseState const& response) -> record {
  auto headers = record{};
  for (auto const& [key, value] : response.headers) {
    headers[key] = value;
  }
  return record{
    {"code", static_cast<uint64_t>(response.status)},
    {"headers", std::move(headers)},
  };
}

// Normalizes platform-specific socket error text for user-facing diagnostics.
// In particular, `AsyncSocketException` embeds OS-dependent errno values
// (e.g., 111 on Linux vs. 61 on macOS) that make error messages noisy and
// non-portable in tests.
auto strip_errno_from_error_message(std::string message) -> std::string {
  auto needle = std::string_view{"errno = "};
  auto pos = message.find(needle);
  if (pos == std::string::npos) {
    return message;
  }
  auto end = message.find('(', pos);
  if (end == std::string::npos) {
    return message;
  }
  auto start = pos + needle.length();
  message.erase(start, end - start);
  return message;
}

auto find_content_encoding(
  std::vector<std::pair<std::string, std::string>> const& headers)
  -> Option<std::string> {
  for (auto const& [name, value] : headers) {
    if (detail::ascii_icase_equal(name, "content-encoding")) {
      auto trimmed = std::string{detail::trim(value)};
      if (not trimmed.empty()) {
        return trimmed;
      }
    }
  }
  return None{};
}

struct retryable_http_response : std::runtime_error {
  retryable_http_response(
    uint16_t status_code,
    std::vector<std::pair<std::string, std::string>> headers,
    Option<std::chrono::seconds> retry_after)
    : std::runtime_error{fmt::format("retryable HTTP status {}", status_code)},
      status_code{status_code},
      headers{std::move(headers)},
      retry_after{retry_after} {
  }

  uint16_t status_code = 0;
  std::vector<std::pair<std::string, std::string>> headers;
  Option<std::chrono::seconds> retry_after;
  blob body;
};

// Static fetch task that runs on the Proxygen EventBase thread. All
// results are communicated through the message queue; no operator members
// are touched from this task.
auto fetch(folly::EventBase* evb, proxygen::URL url, RequestConfig request,
           FetchConfig config, bool buffer_retryable_body, Arc<MessageQueue> mq)
  -> Task<void> {
  co_return co_await folly::coro::co_withExecutor(
    evb,
    folly::coro::co_invoke([evb, url = std::move(url),
                            request = std::move(request),
                            config = std::move(config), buffer_retryable_body,
                            mq = std::move(mq)]() mutable -> Task<void> {
      auto result = co_await folly::coro::co_awaitTry([&]() -> Task<void> {
        auto const& host = url.getHost();
        auto const is_secure = url.isSecure();
        // Build connection params once (outside the retry loop).
        auto conn_params = proxygen::coro::HTTPClient::getConnParams(
          is_secure ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                    : proxygen::coro::HTTPClient::SecureTransportImpl::NONE,
          host);
        if (is_secure and config.tls_context) {
          conn_params.sslContext = config.tls_context;
        }
        auto sess_params = proxygen::coro::HTTPClient::getSessionParams(
          config.request_timeout);
        // Retry on socket-level failures and retryable HTTP status codes.
        // A diagnostic warning is emitted before each retry attempt.
        auto emitted_messages = false;
        auto retries_done = uint32_t{0};
        co_await folly::coro::retryWhen(
          [&]() -> Task<void> {
            emitted_messages = false;
            // Resolve DNS and establish the TCP connection.
            auto addresses
              = co_await proxygen::coro::CoroDNSResolver::resolveHost(
                evb, host, config.connection_timeout);
            auto server_addr = std::move(addresses.primary);
            server_addr.setPort(url.getPort());
            auto* session = co_await proxygen::coro::HTTPCoroConnector::connect(
              evb, server_addr, config.connection_timeout, conn_params,
              sess_params);
            auto holder = session->acquireKeepAlive();
            SCOPE_EXIT {
              if (auto* s = holder.get()) {
                s->initiateDrain();
              }
            };
            auto reservation = session->reserveRequest();
            if (reservation.hasException()) {
              co_yield folly::coro::co_error(
                std::move(reservation.exception()));
            }
            // Build a streaming reader that feeds messages into the queue.
            auto reader = proxygen::coro::HTTPSourceReader{};
            Option<retryable_http_response> retryable_response;
            reader
              .onHeadersAsync([mq, &emitted_messages, &retryable_response,
                               buffer_retryable_body](
                                std::unique_ptr<proxygen::HTTPMessage> msg,
                                bool is_final,
                                bool) mutable -> folly::coro::Task<bool> {
                if (not is_final) {
                  // Ignore informational 1xx headers and wait for final
                  // response headers.
                  co_return proxygen::coro::HTTPSourceReader::Continue;
                }
                auto hdrs = std::vector<std::pair<std::string, std::string>>{};
                msg->getHeaders().forEach([&](std::string& k, std::string& v) {
                  hdrs.emplace_back(k, v);
                });
                auto status = msg->getStatusCode();
                if (http::is_retryable_http_status(status)) {
                  auto retry_after = http::parse_retry_after(
                    msg->getHeaders().getSingleOrEmpty("Retry-After"));
                  retryable_response = retryable_http_response{
                    status, std::move(hdrs), retry_after};
                  if (not buffer_retryable_body) {
                    throw std::move(*retryable_response);
                  }
                  co_return proxygen::coro::HTTPSourceReader::Continue;
                }
                emitted_messages = true;
                co_await mq->enqueue(ResponseHeader{status, std::move(hdrs)});
                co_return proxygen::coro::HTTPSourceReader::Continue;
              })
              .onBodyAsync([mq, &emitted_messages, &retryable_response](
                             quic::BufQueue buf_queue,
                             bool) mutable -> folly::coro::Task<bool> {
                if (not buf_queue.empty()) {
                  auto iobuf = buf_queue.move();
                  auto bytes = as_bytes(iobuf->coalesce());
                  if (retryable_response) {
                    // buffer body
                    retryable_response->body.insert(
                      retryable_response->body.end(), bytes.begin(),
                      bytes.end());
                  } else {
                    // send as message
                    emitted_messages = true;
                    co_await mq->enqueue(ResponseBody{chunk::copy(bytes)});
                  }
                }
                co_return proxygen::coro::HTTPSourceReader::Continue;
              });
            auto body_buf = std::unique_ptr<folly::IOBuf>{};
            if (not request.body.empty()) {
              body_buf = folly::IOBuf::copyBuffer(request.body.data(),
                                                  request.body.size());
            }
            auto req_source = build_request_source(
              url, request.method, request.headers, std::move(body_buf));
            co_await proxygen::coro::HTTPClient::request(
              session, std::move(*reservation), std::move(req_source),
              std::move(reader), config.request_timeout);
            if (retryable_response) {
              throw std::move(*retryable_response);
            }
          },
          [&](folly::exception_wrapper&& ew) -> Task<void> {
            // Once an attempt emitted response messages, do not retry.
            // Retrying after partial output can duplicate/corrupt downstream
            // parser and pagination state.
            if (emitted_messages) {
              co_yield folly::coro::co_error(std::move(ew));
            }
            auto retryable = false;
            if (ew.is_compatible_with<folly::AsyncSocketException>()) {
              retryable = true;
            }
            if (ew.is_compatible_with<retryable_http_response>()) {
              retryable = true;
            }
            ew.with_exception([&](proxygen::coro::HTTPError const& err) {
              retryable = http::is_retryable_http_error(err.code);
            });
            if (not retryable or retries_done >= config.max_retry_count) {
              co_yield folly::coro::co_error(std::move(ew));
            }
            auto retry_after = Option<std::chrono::seconds>{};
            auto reason = std::string{"connection error"};
            ew.with_exception([&](retryable_http_response const& e) {
              reason = fmt::format("HTTP error {}", e.status_code);
              retry_after = e.retry_after;
            });
            auto delay = http::retry_delay_for_attempt(
              config.retry_delay, retries_done, retry_after);
            // Emit a warning diagnostic before sleeping.
            auto const delay_secs
              = std::chrono::duration_cast<std::chrono::seconds>(delay);
            co_await mq->enqueue(RetryWarning{
              fmt::format("{}, attempt {}/{}, retrying after {}s", reason,
                          retries_done + 1u, config.max_retry_count + 1u,
                          delay_secs.count())});
            co_await folly::coro::sleep(delay);
            ++retries_done;
          });
      }());
      if (result.hasException()) {
        if (auto const* res
            = result.exception().get_exception<retryable_http_response>()) {
          co_await mq->enqueue(ResponseHeader{res->status_code, res->headers});
          if (not res->body.empty()) {
            co_await mq->enqueue(
              ResponseBody{chunk::copy(res->body.data(), res->body.size())});
          }
        } else {
          co_await mq->enqueue(
            FetchError{result.exception().what().toStdString()});
        }
      }
      co_await mq->enqueue(FetchDone{});
    }));
}

// ---- Operator ---------------------------------------------------------------

class FromHttp final : public Operator<void, table_slice> {
public:
  explicit FromHttp(FromHttpArgs args)
    : message_queue_{std::in_place, 64}, args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    // prepare pagination
    auto paginate = validate_paginate(args_.paginate, ctx.dh());
    if (not paginate) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    paginate_ = std::move(*paginate);
    // resolve secrets
    std::string resolved_url;
    auto secret_resolution = co_await resolve_http_secrets(
      ctx, args_, resolved_url, resolved_headers_);
    if (secret_resolution.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    pagination_.current_url = resolved_url;
    // prepare fetch config
    fetch_config_ = make_fetch_config(args_);
    // ensure system CA paths are registered
    ensure_http_default_ca_paths();
    co_await start_fetch(ctx, make_request_config(args_, resolved_headers_));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    co_await co_match(
      std::move(result).as<Message>(),
      // --- ResponseHeader ---
      [&](ResponseHeader hdr) -> Task<void> {
        auto headers = std::move(hdr.headers);
        auto content_encoding = find_content_encoding(headers);
        response_ = ResponseState{
          .status = hdr.status,
          .headers = std::move(headers),
          .content_encoding = content_encoding,
          .decompressor = nullptr,
          .error_body = {},
        };
        if (content_encoding) {
          if (auto dec = http::make_decompressor(*content_encoding, ctx)) {
            response_->decompressor = std::move(*dec);
          } else {
            response_->content_encoding = None{};
          }
        }
        co_await spawn_parser(ctx);
        if (lifecycle_ == Lifecycle::done) {
          co_return;
        }
        if (response_->is_success()) {
          if (auto mode = builtin_pagination_mode(paginate_);
              mode and *mode == http::PaginationMode::link) {
            TENZIR_ASSERT(paginate_);
            // Extract the rel=next URL for link pagination.
            pagination_.next_url
              = http::next_url_from_link_headers(response_->headers,
                                                 pagination_.current_url,
                                                 paginate_->source, ctx.dh());
          }
        } else {
          if (not args_.error_field) {
            diagnostic::error("received HTTP error status {}",
                              response_->status)
              .primary(args_.operator_location)
              .emit(ctx);
          }
        }
        co_return;
      },
      // --- ResponseBody ---
      [&](ResponseBody body) -> Task<void> {
        if (not response_body_needed()) {
          co_return;
        }
        auto const* p = reinterpret_cast<std::byte const*>(body.data->data());
        auto payload = blob{p, p + body.data->size()};
        if (response_->decompressor) {
          auto decompressed = http::decompress_chunk(*response_->decompressor,
                                                     payload, ctx.dh());
          if (decompressed.is_err()) {
            co_return;
          }
          payload = std::move(decompressed).unwrap();
        }
        if (payload.empty()) {
          co_return;
        }
        bytes_read_.add(payload.size());
        if (response_->is_success()) {
          // push to parser
          if (auto sub = ctx.get_sub(pagination_.page_count)) {
            auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
            auto push_result = co_await pipeline.push(chunk::copy(
              reinterpret_cast<char const*>(payload.data()), payload.size()));
            TENZIR_UNUSED(push_result);
          }
        } else {
          // append to error body
          response_->error_body.insert(response_->error_body.end(),
                                       payload.begin(), payload.end());
        }
        co_return;
      },
      // --- FetchError ---
      [&](FetchError err) -> Task<void> {
        diagnostic::error(
          "HTTP request to `{}` failed: {}", pagination_.current_url,
          strip_errno_from_error_message(std::move(err.message)))
          .primary(args_.operator_location)
          .emit(ctx);
        pagination_.next_url = None{};
        lifecycle_ = Lifecycle::done;
        co_return;
      },
      // --- RetryWarning ---
      [&](RetryWarning warn) -> Task<void> {
        diagnostic::warning("{}", warn.message)
          .primary(args_.operator_location)
          .emit(ctx);
        co_return;
      },
      // --- FetchDone ---
      [&](FetchDone) -> Task<void> {
        if (response_ and not response_->is_success() and args_.error_field) {
          co_await push_error_field(push, ctx);
        }
        // close sub-pipeline (next pagination request is started from finish_sub)
        if (auto sub = ctx.get_sub(pagination_.page_count)) {
          auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
          co_await pipeline.close();
        }
        co_return;
      });
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    if (auto mode = builtin_pagination_mode(paginate_);
        mode and *mode == http::PaginationMode::odata) {
      TENZIR_ASSERT(paginate_);
      auto page = http::extract_odata_page(slice, paginate_->source, ctx.dh());
      if (not page) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      odata_envelope_seen_ = true;
      pagination_.next_url
        = page->next_url
            ? resolve_next_url(*page->next_url, pagination_.current_url,
                               paginate_->source, ctx.dh(),
                               NextUrlSource::odata_next_link)
            : None{};
      for (auto& event_slice : page->events) {
        auto const rows = event_slice.rows();
        co_await push(std::move(event_slice));
        events_read_.add(rows);
      }
      co_return;
    }
    if (not pagination_.next_url) {
      if (auto next = next_url_from_lambda(paginate_, slice,
                                           pagination_.current_url, ctx.dh())) {
        pagination_.next_url = std::move(*next);
      }
    }
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    events_read_.add(rows);
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (auto mode = builtin_pagination_mode(paginate_);
        mode and *mode == http::PaginationMode::odata
        and not odata_envelope_seen_ and response_
        and response_->is_success()) {
      TENZIR_ASSERT(paginate_);
      diagnostic::error(
        "expected OData response body to contain a top-level `value` array")
        .primary(paginate_->source)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (pagination_.next_url) {
      // next page
      pagination_.current_url = std::move(*pagination_.next_url);
      pagination_.next_url = None{};
      pagination_.page_count += 1;
      if (args_.paginate_delay
          and args_.paginate_delay->inner > duration::zero()) {
        co_await sleep_for(
          std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            args_.paginate_delay->inner));
      }
      co_await start_fetch(ctx,
                           make_paginated_request_config(resolved_headers_));
    } else {
      lifecycle_ = Lifecycle::done;
    }
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  enum class Lifecycle { running, done };

  // Starts the fetch task for the current page.
  // Requires pagination_.current_url and pagination_.page_count to be set.
  auto start_fetch(OpCtx& ctx, RequestConfig request) -> Task<void> {
    response_ = None{};
    odata_envelope_seen_ = false;
    auto parsed_url = proxygen::URL{pagination_.current_url};
    if (parsed_url.isSecure() and not fetch_config_.tls_context) {
      auto tls_opts
        = tls_options::from_optional(args_.tls, {.is_server = false});
      auto result = tls_opts.make_folly_ssl_context(
        ctx, std::addressof(ctx.actor_system().config()), true);
      if (result.is_success()) {
        fetch_config_.tls_context = std::move(*result);
      } else {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
    }
    bytes_read_ = ctx.make_counter(
      MetricsLabel{"host",
                   MetricsLabel::FixedString::truncate(parsed_url.getHost())},
      MetricsDirection::read, MetricsVisibility::external_, MetricsType::bytes);
    events_read_ = ctx.make_counter(
      MetricsLabel{"host",
                   MetricsLabel::FixedString::truncate(parsed_url.getHost())},
      MetricsDirection::read, MetricsVisibility::external_,
      MetricsType::events);
    ctx.spawn_task(fetch(evb_, std::move(parsed_url), std::move(request),
                         fetch_config_, static_cast<bool>(args_.error_field),
                         message_queue_));
    co_return;
  }

  // Spawns the parser sub-pipeline for the current page.
  // Requires response_, pagination_.current_url and pagination_.page_count to
  // be set.
  auto spawn_parser(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(response_);
    auto pipeline = args_.parser.inner;
    auto env = substitute_ctx::env_t{};
    env[args_.response] = make_response_context(*response_);
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    if (not pipeline.substitute(substitute_ctx{b_ctx, &env}, true)) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    co_await ctx.spawn_sub(pagination_.page_count, std::move(pipeline),
                           tag_v<chunk_ptr>);
  }

  auto push_error_field(Push<table_slice>& push, OpCtx& ctx) -> Task<void> {
    auto record_sb = series_builder{};
    std::ignore = record_sb.record();
    auto error_sb = series_builder{};
    error_sb.data(std::move(response_->error_body));
    auto slice = assign(*args_.error_field, error_sb.finish_assert_one_array(),
                        record_sb.finish_assert_one_slice(), ctx.dh());
    co_await push(std::move(slice));
  }

  auto response_body_needed() const -> bool {
    return response_ and (response_->is_success() or args_.error_field);
  }

  // --- transient ---
  mutable Arc<MessageQueue> message_queue_;
  folly::EventBase* evb_{};
  MetricsCounter bytes_read_;
  MetricsCounter events_read_;
  // --- args ---
  FromHttpArgs args_;
  FetchConfig fetch_config_;
  std::vector<std::pair<std::string, std::string>> resolved_headers_;
  Option<pagination_spec> paginate_;
  // --- state ---
  Lifecycle lifecycle_{};
  PaginationState pagination_;
  // State collected per response page; absent before first response header.
  Option<ResponseState> response_;
  bool odata_envelope_seen_ = false;
};

class from_http_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromHttpArgs, FromHttp>{};
    d.positional("url", &FromHttpArgs::url);
    auto method_arg = d.named("method", &FromHttpArgs::method);
    auto body_arg = d.named("body", &FromHttpArgs::body);
    auto headers_arg = d.named("headers", &FromHttpArgs::headers, "record");
    auto tls_validator = tls_options{
      {.is_server = false}}.add_to_describer(d, &FromHttpArgs::tls);
    auto timeout_arg = d.named("timeout", &FromHttpArgs::timeout);
    d.named("error_field", &FromHttpArgs::error_field);
    auto metadata_field_arg
      = d.named("metadata_field", &FromHttpArgs::metadata_field, "any");
    auto paginate_arg = d.named("paginate", &FromHttpArgs::paginate, "any");
    auto paginate_delay_arg
      = d.named("paginate_delay", &FromHttpArgs::paginate_delay);
    auto connection_timeout_arg
      = d.named("connection_timeout", &FromHttpArgs::connection_timeout);
    auto max_retry_count_arg
      = d.named("max_retry_count", &FromHttpArgs::max_retry_count);
    auto retry_delay_arg = d.named("retry_delay", &FromHttpArgs::retry_delay);
    auto encode_arg = d.named("encode", &FromHttpArgs::encode);
    auto server_arg = d.named("server", &FromHttpArgs::server);
    auto parser_arg = d.pipeline(&FromHttpArgs::parser,
                                 {{"response", &FromHttpArgs::response}});
    d.operator_location(&FromHttpArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      // Validate TLS options.
      tls_validator(ctx);
      // `server=true` is not supported; direct users to `accept_http`.
      if (auto server = ctx.get_location(server_arg)) {
        diagnostic::error("`server` is not supported by `from_http` anymore")
          .hint("use `accept_http` to listen for incoming HTTP requests")
          .primary(*server)
          .emit(ctx);
      }
      if (auto metadata_field = ctx.get_location(metadata_field_arg)) {
        diagnostic::error("`metadata_field` has been removed from `from_http`")
          .hint("use `$response` in the parser sub-pipeline instead")
          .primary(*metadata_field)
          .emit(ctx);
      }
      // Validate encode: requires a body and must be "json" or "form".
      auto body_loc = ctx.get_location(body_arg);
      if (auto encode_loc = ctx.get_location(encode_arg)) {
        if (not body_loc) {
          diagnostic::error("`encode` requires a `body`")
            .primary(*encode_loc)
            .emit(ctx);
        } else if (auto encode = ctx.get(encode_arg)) {
          if (encode->inner != "json" and encode->inner != "form") {
            diagnostic::error("unsupported encoding: `{}`", encode->inner)
              .hint(R"(`encode` must be `"json"` or `"form"`)")
              .primary(encode->source)
              .emit(ctx);
          }
        }
      }
      // Validate body type when explicitly provided.
      if (auto body = ctx.get(body_arg)) {
        auto is_valid = match(
          body->inner,
          [](std::string const&) {
            return true;
          },
          [](blob const&) {
            return true;
          },
          [](record const&) {
            return true;
          },
          [](auto const&) {
            return false;
          });
        if (not is_valid) {
          diagnostic::error("`body` must be `blob`, `record`, or `string`")
            .primary(body->source)
            .emit(ctx);
        }
      }
      // Validate header values.
      if (auto headers = ctx.get(headers_arg)) {
        if (auto const* rec = try_as<record>(headers->inner)) {
          for (auto const& [_, value] : *rec) {
            if (not is<std::string>(value) and not is<secret>(value)) {
              diagnostic::error("header values must be `string` or `secret`")
                .primary(headers->source)
                .emit(ctx);
              break;
            }
          }
        }
      }
      // Validate method when explicitly provided.
      if (auto method = ctx.get(method_arg)) {
        if (not proxygen::stringToMethod(method->inner)) {
          diagnostic::error("invalid http method: `{}`", method->inner)
            .primary(method->source)
            .emit(ctx);
        }
      }
      // Validate timeout/retry arguments.
      if (auto timeout = ctx.get(timeout_arg)) {
        if (timeout->inner < duration::zero()) {
          diagnostic::error("`timeout` must be a non-negative duration")
            .primary(timeout->source)
            .emit(ctx);
        }
      }
      if (auto pd = ctx.get(paginate_delay_arg)) {
        if (pd->inner < duration::zero()) {
          diagnostic::error("`paginate_delay` must be a non-negative duration")
            .primary(pd->source)
            .emit(ctx);
        }
      }
      if (auto ct = ctx.get(connection_timeout_arg)) {
        if (ct->inner < duration::zero()) {
          diagnostic::error(
            "`connection_timeout` must be a non-negative duration")
            .primary(ct->source)
            .emit(ctx);
        }
      }
      if (auto rd = ctx.get(retry_delay_arg)) {
        if (rd->inner < duration::zero()) {
          diagnostic::error("`retry_delay` must be a non-negative duration")
            .primary(rd->source)
            .emit(ctx);
        }
      }
      if (auto mrc = ctx.get(max_retry_count_arg)) {
        if (mrc->inner > std::numeric_limits<uint32_t>::max()) {
          diagnostic::error("`max_retry_count` must be <= {}",
                            std::numeric_limits<uint32_t>::max())
            .primary(mrc->source)
            .emit(ctx);
        }
      }
      if (auto paginate_expr = ctx.get(paginate_arg)) {
        auto validated
          = validate_paginate(Option<ast::expression>{*paginate_expr}, ctx);
        if (not validated) {
          return {};
        }
      }
      // Validate that the parser pipeline is present and produces events.
      TRY(auto parser, ctx.get(parser_arg));
      auto output = parser.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (not output.is_error()) {
        if (not *output or (*output)->is_not<table_slice>()) {
          diagnostic::error("pipeline must return events")
            .primary(parser.source.subloc(0, 1))
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_http::from_http_plugin)
