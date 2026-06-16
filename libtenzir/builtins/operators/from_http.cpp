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
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/format_utils.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_auth.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/result.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/try.hpp>
#include <tenzir/variant.hpp>
#include <tenzir/view3.hpp>

#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
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

#include <algorithm>
#include <cctype>
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
  Option<located<std::string>> auth;
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
  Option<located<ir::pipeline>> parser;
  let_id response;
  location operator_location = location::unknown;
};

// Messages from the fetch task to the operator.
struct ResponseHeader {
  uint16_t status;
  std::vector<http::Header> headers;
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
auto build_request_source(proxygen::URL const& url, proxygen::HTTPMethod method,
                          const std::vector<http::Header>& headers,
                          std::unique_ptr<folly::IOBuf> body_buf)
  -> proxygen::coro::HTTPSourceHolder {
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
  proxygen::HTTPMethod method = proxygen::HTTPMethod::GET;
  std::vector<http::Header> headers;
  std::vector<std::byte> body;
};

auto parse_http_method(std::string method_str) -> Option<proxygen::HTTPMethod> {
  std::transform(method_str.begin(), method_str.end(), method_str.begin(),
                 [](unsigned char c) {
                   return static_cast<char>(std::toupper(c));
                 });
  if (auto method = proxygen::stringToMethod(method_str)) {
    return *method;
  }
  return None{};
}

struct BodySerialization {
  std::vector<std::byte> body;
  Option<std::string> content_type;
};

auto serialize_body(data const& value,
                    Option<located<std::string>> const& encode)
  -> Option<BodySerialization> {
  auto result = BodySerialization{};
  auto valid = match(
    value,
    [&](std::string const& s) {
      auto const* p = reinterpret_cast<std::byte const*>(s.data());
      result.body.insert(result.body.end(), p, p + s.size());
      return true;
    },
    [&](blob const& b) {
      result.body.insert(result.body.end(), b.begin(), b.end());
      return true;
    },
    [&](record const& r) {
      if (encode and encode->inner == "form") {
        auto buf = curl::escape(flatten(r));
        auto const* p = reinterpret_cast<std::byte const*>(buf.data());
        result.body.insert(result.body.end(), p, p + buf.size());
        result.content_type = "application/x-www-form-urlencoded";
      } else {
        auto buf = std::string{};
        auto printer = json_printer{{}};
        auto out = std::back_inserter(buf);
        printer.print(out, r);
        auto const* p = reinterpret_cast<std::byte const*>(buf.data());
        result.body.insert(result.body.end(), p, p + buf.size());
        result.content_type = "application/json";
      }
      return true;
    },
    [](auto const&) {
      return false;
    });
  if (not valid) {
    return None{};
  }
  return result;
}

auto add_default_content_type(std::vector<http::Header>& headers,
                              Option<std::string> content_type) -> void {
  if (content_type and not http::find(headers, "content-type")) {
    headers.emplace_back("Content-Type", content_type.unwrap());
  }
}

// Builds the initial request configuration from the operator arguments.
// Resolves method, serializes the body (records become JSON, blobs are sent
// verbatim), and assembles request headers.
auto make_request_config(FromHttpArgs const& args,
                         std::vector<http::Header> headers,
                         Option<data> const& resolved_body) -> RequestConfig {
  // Resolve HTTP method (validated at describe time).
  // Default to POST when a body is present, GET otherwise.
  auto method_str = args.method ? args.method->inner
                    : args.body ? "POST"
                                : "GET";
  auto method = parse_http_method(method_str);
  TENZIR_ASSERT(method);
  // Serialize the optional body. Records become JSON; blobs and strings are
  // sent verbatim.
  auto body = std::vector<std::byte>{};
  if (resolved_body) {
    auto serialized = serialize_body(*resolved_body, args.encode);
    TENZIR_ASSERT(serialized);
    body = std::move(serialized->body);
    add_default_content_type(headers, std::move(serialized->content_type));
  }
  if (not http::find(headers, "accept")) {
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
auto make_paginated_request_config(std::vector<http::Header> headers)
  -> RequestConfig {
  if (not http::find(headers, "accept")) {
    headers.emplace_back("Accept", "application/json, */*;q=0.5");
  }
  return RequestConfig{
    .method = proxygen::HTTPMethod::GET,
    .headers = std::move(headers),
    .body = {},
  };
}

auto resolve_http_secrets(OpCtx& ctx, FromHttpArgs const& args,
                          std::string& resolved_url,
                          std::vector<http::Header>& resolved_headers,
                          Option<data>& resolved_body)
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
  if (args.body) {
    resolved_body = args.body->inner;
    auto body_requests
      = make_secret_request(*resolved_body, args.body->source, ctx.dh());
    requests.insert(requests.end(),
                    std::make_move_iterator(body_requests.begin()),
                    std::make_move_iterator(body_requests.end()));
  }
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return failure::promise();
  }
  if (resolved_url.empty()) {
    diagnostic::error("`url` must not be empty").primary(args.url).emit(ctx);
    co_return failure::promise();
  }
  CO_TRY(auto tls_enabled,
         http::normalize_url_and_tls(args.tls, resolved_url, args.url.source,
                                     ctx, ctx.actor_system().config()));
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

struct NextRequest {
  std::string url;
  RequestConfig request;
};

struct PaginationState {
  Option<pagination_spec> spec;
  int64_t page_count = 0;
  std::string current_url;
  RequestConfig current_request;
  Option<NextRequest> next_request;
  bool odata_envelope_seen = false;
};

struct PaginationRequestContext {
  pagination_spec const& spec;
  std::string const& current_url;
  RequestConfig const& current_request;
  std::vector<http::Header> const& paginated_headers;
  Option<located<std::string>> const& encode;
};

auto emit_paginate_record_warning(std::string message, location paginate_loc,
                                  diagnostic_handler& dh) -> void {
  diagnostic::warning("{}", message)
    .primary(paginate_loc)
    .note("stopping pagination")
    .emit(dh);
}

auto is_paginate_request_field(std::string_view field) -> bool {
  return field == "url" or field == "method" or field == "headers"
         or field == "body";
}

auto next_request_from_record(record const& patch,
                              PaginationRequestContext const& context,
                              diagnostic_handler& dh) -> Option<NextRequest> {
  if (patch.empty()) {
    emit_paginate_record_warning("`paginate` request record must not be empty",
                                 context.spec.source, dh);
    return None{};
  }
  for (auto const& [field, _] : patch) {
    if (not is_paginate_request_field(field)) {
      emit_paginate_record_warning(
        fmt::format("unknown field `{}` in `paginate` request record", field),
        context.spec.source, dh);
      return None{};
    }
  }
  auto url = context.current_url;
  auto request = context.current_request;
  if (auto it = patch.find("url"); it != patch.end()) {
    auto const* next_url = try_as<std::string>(&it->second);
    if (not next_url) {
      emit_paginate_record_warning("`paginate` request field `url` must be "
                                   "`string`",
                                   context.spec.source, dh);
      return None{};
    }
    if (auto resolved
        = resolve_next_url(*next_url, context.current_url, context.spec.source,
                           dh, NextUrlSource::paginate_lambda)) {
      url = std::move(*resolved);
    } else {
      return None{};
    }
  }
  if (auto it = patch.find("method"); it != patch.end()) {
    auto const* method_str = try_as<std::string>(&it->second);
    if (not method_str) {
      emit_paginate_record_warning("`paginate` request field `method` must be "
                                   "`string`",
                                   context.spec.source, dh);
      return None{};
    }
    auto method = parse_http_method(*method_str);
    if (not method) {
      emit_paginate_record_warning(
        fmt::format("invalid HTTP method in `paginate` request record: `{}`",
                    *method_str),
        context.spec.source, dh);
      return None{};
    }
    request.method = *method;
  }
  if (auto it = patch.find("body"); it != patch.end()) {
    if (is<caf::none_t>(it->second)) {
      request.body.clear();
    } else if (auto serialized = serialize_body(it->second, context.encode)) {
      request.body = std::move(serialized->body);
      add_default_content_type(request.headers,
                               std::move(serialized->content_type));
    } else {
      emit_paginate_record_warning("`paginate` request field `body` must be "
                                   "`blob`, `record`, `string`, "
                                   "or `null`",
                                   context.spec.source, dh);
      return None{};
    }
  }
  if (auto it = patch.find("headers"); it != patch.end()) {
    auto const* headers = try_as<record>(&it->second);
    if (not headers) {
      emit_paginate_record_warning("`paginate` request field `headers` must be "
                                   "`record`",
                                   context.spec.source, dh);
      return None{};
    }
    for (auto const& [name, value] : *headers) {
      if (is<caf::none_t>(value)) {
        http::erase(request.headers, name);
      } else if (auto const* str = try_as<std::string>(&value)) {
        http::set(request.headers, name, *str);
      } else {
        emit_paginate_record_warning("`paginate` request header values must be "
                                     "`string` or `null`",
                                     context.spec.source, dh);
        return None{};
      }
    }
  }
  return NextRequest{.url = std::move(url), .request = std::move(request)};
}

auto next_request_from_url(std::string_view url,
                           PaginationRequestContext const& context,
                           diagnostic_handler& dh) -> Option<NextRequest> {
  if (auto resolved
      = resolve_next_url(url, context.current_url, context.spec.source, dh,
                         NextUrlSource::paginate_lambda)) {
    return NextRequest{
      .url = std::move(*resolved),
      .request = make_paginated_request_config(context.paginated_headers),
    };
  }
  return None{};
}

auto next_request_from_lambda(PaginationRequestContext const& context,
                              table_slice const& slice, diagnostic_handler& dh)
  -> Option<NextRequest> {
  auto const* lambda = try_as<ast::lambda_expr>(&context.spec.inner);
  if (not lambda) {
    return None{};
  }
  if (slice.rows() != 1) {
    diagnostic::warning("cannot paginate over multiple events")
      .primary(context.spec)
      .note("stopping pagination")
      .emit(dh);
    return None{};
  }
  auto ms = eval(*lambda, series{slice}, dh);
  auto value = ms.view3_at(0);
  return match(
    value,
    [](caf::none_t const&) -> Option<NextRequest> {
      return None{};
    },
    [&](std::string_view url) -> Option<NextRequest> {
      return next_request_from_url(url, context, dh);
    },
    [&](view3<record> req) -> Option<NextRequest> {
      return next_request_from_record(materialize(req), context, dh);
    },
    [&](auto const&) -> Option<NextRequest> {
      diagnostic::error("expected `paginate` to be `string`, `record`, or "
                        "`null`, got `{}`",
                        ms.parts().front().type.kind())
        .primary(context.spec)
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

struct ResponseState {
  uint16_t status;
  std::vector<http::Header> headers;
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

auto describe_socket_error(folly::AsyncSocketException const& ex)
  -> std::string {
  if (auto err = ex.getErrno(); err > 0) {
    return folly::errnoStr(err);
  }
  return ex.what();
}

auto describe_fetch_error(folly::exception_wrapper const& ew) -> std::string {
  if (auto const* ex = ew.get_exception<folly::AsyncSocketException>()) {
    return describe_socket_error(*ex);
  }
  return ew.what().toStdString();
}

auto path_from_url(std::string_view url) -> std::string {
  auto parsed = boost::urls::parse_uri_reference(url);
  if (not parsed) {
    return {};
  }
  return std::string{parsed->path()};
}

auto make_parser_pipeline(operator_factory_plugin const& plugin, location loc,
                          OpCtx& ctx) -> failure_or<ir::pipeline> {
  auto ast = ast::pipeline{};
  ast.body.emplace_back(invocation_for_plugin(plugin, loc));
  auto root = compile_ctx::make_root(ctx);
  return std::move(ast).compile(root);
}

struct retryable_http_response : std::runtime_error {
  retryable_http_response(uint16_t status_code,
                          std::vector<http::Header> headers,
                          Option<std::chrono::seconds> retry_after)
    : std::runtime_error{fmt::format("retryable HTTP status {}", status_code)},
      status_code{status_code},
      headers{std::move(headers)},
      retry_after{retry_after} {
  }

  uint16_t status_code = 0;
  std::vector<http::Header> headers;
  Option<std::chrono::seconds> retry_after;
  blob body;
};

struct auth_retry_response : std::runtime_error {
  explicit auth_retry_response(std::vector<http::Header> headers)
    : std::runtime_error{"auth retry"}, headers{std::move(headers)} {
  }

  std::vector<http::Header> headers;
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
                auto hdrs = std::vector<http::Header>{};
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
            FetchError{describe_fetch_error(result.exception())});
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
    pagination_.spec = std::move(*paginate);
    // resolve secrets
    std::string resolved_url;
    auto resolved_body = Option<data>{};
    auto secret_resolution = co_await resolve_http_secrets(
      ctx, args_, resolved_url, resolved_headers_, resolved_body);
    if (secret_resolution.is_error()) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    pagination_.current_url = resolved_url;
    // prepare fetch config
    fetch_config_ = make_fetch_config(args_);
    // ensure system CA paths are registered
    http::ensure_default_ca_paths();
    pagination_.current_request
      = make_request_config(args_, resolved_headers_, resolved_body);
    co_await start_fetch(ctx, pagination_.current_request);
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
        auto content_encoding = Option<std::string>{};
        if (auto value = http::find(headers, "content-encoding")) {
          auto trimmed = std::string{detail::trim(*value)};
          if (not trimmed.empty()) {
            content_encoding = std::move(trimmed);
          }
        }
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
        if (response_->is_success()) {
          co_await spawn_parser(ctx);
          if (lifecycle_ == Lifecycle::done) {
            co_return;
          }
          if (auto mode = builtin_pagination_mode(pagination_.spec);
              mode and *mode == http::PaginationMode::link) {
            TENZIR_ASSERT(pagination_.spec);
            // Extract the rel=next URL for link pagination.
            if (auto next_url = http::next_url_from_link_headers(
                  response_->headers, pagination_.current_url,
                  pagination_.spec->source, ctx.dh())) {
              pagination_.next_request = NextRequest{
                .url = std::move(*next_url),
                .request = make_paginated_request_config(resolved_headers_),
              };
            }
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
        diagnostic::error("HTTP request to `{}` failed: {}",
                          pagination_.current_url, err.message)
          .primary(args_.operator_location)
          .emit(ctx);
        pagination_.next_request = None{};
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
        } else {
          lifecycle_ = Lifecycle::done;
        }
        co_return;
      });
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    if (auto mode = builtin_pagination_mode(pagination_.spec);
        mode and *mode == http::PaginationMode::odata) {
      TENZIR_ASSERT(pagination_.spec);
      auto page
        = http::extract_odata_page(slice, pagination_.spec->source, ctx.dh());
      if (not page) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      pagination_.odata_envelope_seen = true;
      if (page->next_url) {
        if (auto next_url
            = resolve_next_url(*page->next_url, pagination_.current_url,
                               pagination_.spec->source, ctx.dh(),
                               NextUrlSource::odata_next_link)) {
          pagination_.next_request = NextRequest{
            .url = std::move(*next_url),
            .request = make_paginated_request_config(resolved_headers_),
          };
        }
      }
      for (auto& event_slice : page->events) {
        auto const rows = event_slice.rows();
        co_await push(std::move(event_slice));
        events_read_.add(rows);
      }
      co_return;
    }
    if (not pagination_.next_request and pagination_.spec) {
      auto context = PaginationRequestContext{
        .spec = *pagination_.spec,
        .current_url = pagination_.current_url,
        .current_request = pagination_.current_request,
        .paginated_headers = resolved_headers_,
        .encode = args_.encode,
      };
      if (auto next = next_request_from_lambda(context, slice, ctx.dh())) {
        pagination_.next_request = std::move(*next);
      }
    }
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    events_read_.add(rows);
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (auto mode = builtin_pagination_mode(pagination_.spec);
        mode and *mode == http::PaginationMode::odata
        and not pagination_.odata_envelope_seen and response_
        and response_->is_success()) {
      TENZIR_ASSERT(pagination_.spec);
      diagnostic::error(
        "expected OData response body to contain a top-level `value` array")
        .primary(pagination_.spec->source)
        .emit(ctx);
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    if (pagination_.next_request) {
      // next page
      auto next = std::move(*pagination_.next_request);
      pagination_.current_url = std::move(next.url);
      pagination_.current_request = std::move(next.request);
      pagination_.next_request = None{};
      pagination_.page_count += 1;
      if (args_.paginate_delay
          and args_.paginate_delay->inner > duration::zero()) {
        co_await sleep_for(
          std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            args_.paginate_delay->inner));
      }
      co_await start_fetch(ctx, pagination_.current_request);
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
    pagination_.odata_envelope_seen = false;
    auto parsed_url = proxygen::URL{pagination_.current_url};
    if (parsed_url.isSecure() and not fetch_config_.tls_context) {
      auto tls_opts
        = tls_options::from_optional(args_.tls, {.is_server = false});
      auto tls = tls_opts.resolve(ctx.actor_system().config(), ctx);
      if (tls.is_error()) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      auto result = tls->make_folly_ssl_context(ctx, true);
      if (result.is_success()) {
        fetch_config_.tls_context = std::move(*result);
      } else {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
    }
    if (args_.auth) {
      auto auth = co_await fetch_authorization(args_.auth->inner, ctx);
      if (not auth) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      for (auto const& header : auth->headers) {
        http::set(request.headers, header.name, header.value);
      }
    }
    bytes_read_ = ctx.make_counter(
      MetricsLabel{"host",
                   MetricsLabel::FixedString::truncate(parsed_url.getHost())},
      MetricsDirection::read, MetricsVisibility::external_, MetricsUnit::bytes);
    events_read_ = ctx.make_counter(
      MetricsLabel{"host",
                   MetricsLabel::FixedString::truncate(parsed_url.getHost())},
      MetricsDirection::read, MetricsVisibility::external_,
      MetricsUnit::events);
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
    auto pipeline = ir::pipeline{};
    if (args_.parser) {
      pipeline = args_.parser->inner;
      auto env = substitute_ctx::env_t{};
      env[args_.response] = make_response_context(*response_);
      if (not pipeline.substitute(substitute_ctx{ctx, &env}, true)) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
    } else {
      auto const* plugin = static_cast<const operator_factory_plugin*>(nullptr);
      if (auto value = http::find(response_->headers, "content-type");
          value and not detail::trim(*value).empty()) {
        auto content_type = std::string{detail::trim(*value)};
        plugin = read_plugin_for_content_type(content_type);
        if (not plugin) {
          diagnostic::error("unsupported Content-Type `{}`", content_type)
            .primary(args_.operator_location)
            .hint("pass an explicit parser pipeline")
            .emit(ctx);
          lifecycle_ = Lifecycle::done;
          co_return;
        }
      } else {
        auto path = path_from_url(pagination_.current_url);
        plugin = read_plugin_for_url_path(path);
        if (not plugin) {
          diagnostic::error("could not infer parser for URL path `{}`", path)
            .primary(args_.operator_location)
            .hint("pass an explicit parser pipeline")
            .emit(ctx);
          lifecycle_ = Lifecycle::done;
          co_return;
        }
      }
      auto inferred
        = make_parser_pipeline(*plugin, args_.operator_location, ctx);
      if (inferred.is_error()) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      pipeline = std::move(*inferred);
      TENZIR_TRACE("from_http inferred parser `{}` for `{}`", plugin->name(),
                   pagination_.current_url);
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
    auto const rows = slice.rows();
    co_await push(std::move(slice));
    events_read_.add(rows);
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
  std::vector<http::Header> resolved_headers_;
  // --- state ---
  Lifecycle lifecycle_{};
  PaginationState pagination_;
  // State collected per response page; absent before first response header.
  Option<ResponseState> response_;
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
    auto auth_arg = d.named("auth", &FromHttpArgs::auth);
    auto server_arg = d.named("server", &FromHttpArgs::server);
    auto parser_arg
      = d.pipeline(&FromHttpArgs::parser, SubOptimize::from_downstream,
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
      auto paginate_expr = ctx.get(paginate_arg);
      auto paginate_is_lambda
        = paginate_expr and try_as<ast::lambda_expr>(*paginate_expr);
      if (auto encode_loc = ctx.get_location(encode_arg)) {
        if (not body_loc and not paginate_is_lambda) {
          diagnostic::error("`encode` requires a `body`")
            .primary(*encode_loc)
            .emit(ctx);
        }
        if (auto encode = ctx.get(encode_arg)) {
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
      if (auto auth = ctx.get(auth_arg); auth and auth->inner.empty()) {
        diagnostic::error("`auth` must not be empty")
          .primary(auth->source)
          .emit(ctx);
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
      if (paginate_expr) {
        auto validated
          = validate_paginate(Option<ast::expression>{*paginate_expr}, ctx);
        if (not validated) {
          return {};
        }
      }
      // Validate that an explicit parser pipeline produces events.
      if (auto parser = ctx.get(parser_arg)) {
        auto output = parser->inner.infer_type(tag_v<chunk_ptr>, ctx);
        if (not output.is_error()) {
          if (not *output or (*output)->is_not<table_slice>()) {
            diagnostic::error("pipeline must return events")
              .primary(parser->source.subloc(0, 1))
              .emit(ctx);
          }
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
