//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/result.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
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
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::from_http {

namespace {

constexpr auto message_queue_capacity = uint32_t{256};
constexpr auto default_timeout = std::chrono::milliseconds{90 * 1000};
constexpr auto default_connection_timeout = std::chrono::milliseconds{5 * 1000};
constexpr auto default_retry_delay = std::chrono::milliseconds{1 * 1000};

struct FromHttpArgs {
  located<secret> url;
  Option<located<std::string>> method;
  Option<located<data>> body;
  Option<located<data>> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<ast::field_path> error_field;
  Option<ast::expression> paginate;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  Option<located<std::string>> encode;
  Option<location> server;
  located<ir::pipeline> parser;
  let_id response;
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

using Message = variant<ResponseHeader, ResponseBody, FetchError, FetchDone>;
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
// Resolves method, serialises the body (records become JSON, blobs are sent
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
  // Serialise the optional body. Records become JSON; blobs and strings are
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

auto add_default_url_scheme(std::string& url, bool tls_enabled) -> void {
  if (not url.starts_with("http://") and not url.starts_with("https://")) {
    url.insert(0, tls_enabled ? "https://" : "http://");
  }
}

auto tls_enabled_from_args(FromHttpArgs const& args) -> bool {
  if (not args.tls) {
    return true;
  }
  // Mirror tls_options semantics for explicit `tls=...` arguments:
  // - `tls=true` enables TLS
  // - record values (e.g. `{skip_peer_verification: true}`) implicitly enable
  //   TLS
  // Keep the default false for absent `tls` args when normalizing schemeless
  // URLs.
  auto tls_opts
    = tls_options{*args.tls, {.tls_default = true, .is_server = false}};
  return tls_opts.get_tls(nullptr).inner;
}

auto resolve_http_secrets(
  OpCtx& ctx, FromHttpArgs const& args, std::string& resolved_url,
  std::vector<std::pair<std::string, std::string>>& resolved_headers)
  -> Task<bool> {
  resolved_url.clear();
  resolved_headers.clear();
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(
    make_secret_request("url", args.url, resolved_url, ctx.dh()));
  if (args.headers) {
    if (auto const* rec = try_as<record>(args.headers->inner)) {
      for (auto const& [name, value] : *rec) {
        match(
          value,
          [&](std::string const& literal) {
            resolved_headers.emplace_back(name, literal);
          },
          [&](secret const& sec) {
            auto& out
              = resolved_headers.emplace_back(name, std::string{}).second;
            requests.emplace_back(make_secret_request(
              name, sec, args.headers->source, out, ctx.dh()));
          },
          [](auto const&) {
            TENZIR_UNREACHABLE();
          });
      }
    }
  }
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return false;
  }
  if (resolved_url.empty()) {
    diagnostic::error("`url` must not be empty").primary(args.url).emit(ctx);
    co_return false;
  }
  add_default_url_scheme(resolved_url, tls_enabled_from_args(args));
  co_return true;
}

using pagination_spec = located<variant<ast::lambda_expr, std::string>>;

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
      variant<ast::lambda_expr, std::string>{*lambda},
      expr->get_location(),
    }};
  }
  TRY(auto value, const_eval(*expr, dh));
  return match(
    value,
    [&](std::string const& mode) -> failure_or<Option<pagination_spec>> {
      if (mode != "link") {
        diagnostic::error("unsupported pagination mode: `{}`", mode)
          .primary(*expr)
          .hint("`paginate` must be `\"link\"` or a lambda")
          .emit(dh);
        return failure::promise();
      }
      return Option<pagination_spec>{pagination_spec{
        variant<ast::lambda_expr, std::string>{mode},
        expr->get_location(),
      }};
    },
    [&](auto const&) -> failure_or<Option<pagination_spec>> {
      diagnostic::error("expected `paginate` to be `string` or `lambda`")
        .primary(*expr)
        .hint("`paginate` must be `\"link\"` or a lambda")
        .emit(dh);
      return failure::promise();
    });
}

auto resolve_paginate_url(std::string_view next_url,
                          std::string const& base_url, location paginate_loc,
                          diagnostic_handler& dh) -> Option<std::string> {
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
    diagnostic::warning("invalid next URL from `paginate` lambda: {}",
                        ref.error().message())
      .primary(paginate_loc)
      .note("stopping pagination")
      .emit(dh);
    return None{};
  }
  auto resolved = boost::urls::url{};
  if (auto r = boost::urls::resolve(*base, *ref, resolved); not r) {
    diagnostic::warning("failed to resolve next URL from `paginate` lambda: {}",
                        r.error().message())
      .primary(paginate_loc)
      .note("stopping pagination")
      .emit(dh);
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
      return resolve_paginate_url(url, base_url, paginate->source, dh);
    },
    [&](auto const&) -> Option<std::string> {
      diagnostic::error("expected `paginate` to be `string`, got `{}`",
                        ms.parts().front().type.kind())
        .primary(*paginate)
        .emit(dh);
      return None{};
    });
}

auto is_link_pagination(Option<pagination_spec> const& paginate) -> bool {
  if (not paginate) {
    return false;
  }
  auto const* mode = try_as<std::string>(&paginate->inner);
  if (not mode) {
    return false;
  }
  TENZIR_ASSERT(*mode == "link");
  return true;
}

// ---- RFC 8288 Link header parsing -----------------------------------------
//
// Ported from the legacy http.cpp implementation.

// Splits a raw HTTP Link header value into individual link-value items at
// top-level commas (not inside <...> or quoted strings).
auto split_link_header(std::string_view value)
  -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  auto start = size_t{0};
  auto in_angle = false;
  auto in_quotes = false;
  auto escaped = false;
  for (auto i = size_t{}; i < value.size(); ++i) {
    const auto c = value[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (c == '\\' and in_quotes) {
      escaped = true;
      continue;
    }
    if (c == '"' and not in_angle) {
      in_quotes = not in_quotes;
      continue;
    }
    if (c == '<' and not in_quotes) {
      in_angle = true;
      continue;
    }
    if (c == '>' and not in_quotes) {
      in_angle = false;
      continue;
    }
    if (c == ',' and not in_quotes and not in_angle) {
      auto item = detail::trim(value.substr(start, i - start));
      if (not item.empty()) {
        result.push_back(item);
      }
      start = i + 1;
    }
  }
  auto item = detail::trim(value.substr(start));
  if (not item.empty()) {
    result.push_back(item);
  }
  return result;
}

// Splits link-value parameters at semicolons, honouring quoted-string escaping.
auto split_link_params(std::string_view value)
  -> std::pair<std::vector<std::string_view>, bool> {
  auto result = std::vector<std::string_view>{};
  auto start = size_t{0};
  auto in_quotes = false;
  auto escaped = false;
  for (auto i = size_t{}; i < value.size(); ++i) {
    const auto c = value[i];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (c == '\\' and in_quotes) {
      escaped = true;
      continue;
    }
    if (c == '"') {
      in_quotes = not in_quotes;
      continue;
    }
    if (c == ';' and not in_quotes) {
      auto item = detail::trim(value.substr(start, i - start));
      if (not item.empty()) {
        result.push_back(item);
      }
      start = i + 1;
    }
  }
  auto item = detail::trim(value.substr(start));
  if (not item.empty()) {
    result.push_back(item);
  }
  return {std::move(result), not in_quotes};
}

// Returns true when "next" appears as a token in a rel parameter value.
auto rel_contains_next(std::string_view value) -> bool {
  value = detail::trim(value);
  if (value.size() >= 2 and value.front() == '"' and value.back() == '"') {
    value.remove_prefix(1);
    value.remove_suffix(1);
  }
  auto index = size_t{};
  while (index < value.size()) {
    while (index < value.size()
           and std::isspace(static_cast<unsigned char>(value[index])) != 0) {
      ++index;
    }
    const auto token_begin = index;
    while (index < value.size()
           and std::isspace(static_cast<unsigned char>(value[index])) == 0) {
      ++index;
    }
    const auto token = value.substr(token_begin, index - token_begin);
    if (not token.empty() and detail::ascii_icase_equal(token, "next")) {
      return true;
    }
  }
  return false;
}

// Parses a single RFC 8288 link-value and extracts its `rel=next` target URI.
// Returns Err(Unit) for malformed link-values.
auto next_link_target(std::string_view header)
  -> Result<Option<std::string_view>, Unit> {
  auto item = detail::trim(header);
  if (item.empty()) {
    return None{};
  }
  if (item.front() != '<') {
    return Err{Unit{}};
  }
  const auto uri_end = item.find('>');
  if (uri_end == std::string_view::npos) {
    return Err{Unit{}};
  }
  auto target = item.substr(1, uri_end - 1);
  auto params = item.substr(uri_end + 1);
  auto [param_parts, ok] = split_link_params(params);
  if (not ok) {
    return Err{Unit{}};
  }
  auto has_next = false;
  for (const auto part : param_parts) {
    const auto eq = part.find('=');
    auto name = detail::trim(part.substr(0, eq));
    if (name.empty() or not detail::ascii_icase_equal(name, "rel")) {
      continue;
    }
    if (eq == std::string_view::npos) {
      continue;
    }
    if (rel_contains_next(detail::trim(part.substr(eq + 1)))) {
      has_next = true;
      break;
    }
  }
  if (has_next) {
    return Option<std::string_view>{target};
  }
  return None{};
}

// Scans all Link response headers and returns the first resolved `rel=next`
// URL, or None if no such link is present. Emits a warning on malformed
// headers.
auto next_url_from_link_headers(
  std::vector<std::pair<std::string, std::string>> const& response_headers,
  std::string const& base_url, location paginate_loc, diagnostic_handler& dh)
  -> Option<std::string> {
  auto base = boost::urls::parse_uri_reference(base_url);
  if (not base) {
    diagnostic::warning("failed to parse request URI for link pagination: {}",
                        base.error().message())
      .primary(paginate_loc)
      .note("stopping pagination")
      .emit(dh);
    return None{};
  }
  auto malformed = false;
  for (auto const& [name, value] : response_headers) {
    if (not detail::ascii_icase_equal(name, "link")) {
      continue;
    }
    for (auto header : split_link_header(value)) {
      auto parsed = next_link_target(header);
      if (parsed.is_err()) {
        malformed = true;
        continue;
      }
      auto target = parsed.unwrap();
      if (not target) {
        continue;
      }
      auto ref = boost::urls::parse_uri_reference(*target);
      if (not ref) {
        diagnostic::warning("invalid `rel=next` URL in Link header: {}",
                            ref.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        return None{};
      }
      auto resolved = boost::urls::url{};
      if (auto r = boost::urls::resolve(*base, *ref, resolved); not r) {
        diagnostic::warning("failed to resolve `rel=next` URL: {}",
                            r.error().message())
          .primary(paginate_loc)
          .note("stopping pagination")
          .emit(dh);
        return None{};
      }
      return Option<std::string>{std::string{resolved.buffer()}};
    }
  }
  if (malformed) {
    diagnostic::warning("failed to parse Link header for pagination")
      .primary(paginate_loc)
      .note("stopping pagination")
      .emit(dh);
  }
  return None{};
}

// ---- Fetch task -----------------------------------------------------------

// Configuration for a single fetch (and its retries).
struct FetchConfig {
  std::chrono::milliseconds request_timeout = default_timeout;
  std::chrono::milliseconds connection_timeout = default_connection_timeout;
  uint32_t max_retry_count = 0;
  std::chrono::milliseconds retry_delay = default_retry_delay;
  std::shared_ptr<folly::SSLContext> tls_context;
};

auto make_fetch_config(FromHttpArgs const& args, diagnostic_handler& dh)
  -> Option<FetchConfig> {
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
  tls_options::options opts = {.is_server = false};
  auto tls_opts = args.tls ? tls_options{*args.tls, opts} : tls_options{opts};
  auto result = tls_opts.make_folly_ssl_context(dh);
  if (result.is_success()) {
    config.tls_context = std::move(*result);
  } else {
    return None{};
  }
  return Option<FetchConfig>{std::move(config)};
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

auto is_retryable_http_error(proxygen::coro::HTTPErrorCode code) -> bool {
  using code_t = proxygen::coro::HTTPErrorCode;
  return code == code_t::TRANSPORT_EOF or code == code_t::TRANSPORT_READ_ERROR
         or code == code_t::TRANSPORT_WRITE_ERROR
         or code == code_t::READ_TIMEOUT or code == code_t::WRITE_TIMEOUT;
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

// Static fetch task that runs on the Proxygen EventBase thread. All
// results are communicated through the message queue; no operator members
// are touched from this task.
auto fetch(folly::EventBase* evb, proxygen::URL url, RequestConfig request,
           FetchConfig config, Arc<MessageQueue> mq) -> Task<void> {
  co_return co_await folly::coro::co_withExecutor(
    evb,
    [](folly::EventBase* evb, proxygen::URL url, RequestConfig request,
       FetchConfig config, Arc<MessageQueue> mq) -> Task<void> {
      auto result = co_await folly::coro::co_awaitTry([&]() -> Task<void> {
        auto const& host = url.getHost();
        auto const is_secure = url.isSecure();
        // Build connection params once (outside the retry loop).
        auto conn_params = proxygen::coro::HTTPClient::getConnParams(
          is_secure ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                    : proxygen::coro::HTTPClient::SecureTransportImpl::NONE,
          host);
        if (config.tls_context) {
          conn_params.sslContext = config.tls_context;
        }
        auto sess_params = proxygen::coro::HTTPClient::getSessionParams(
          config.request_timeout);
        // Retry only on socket-level failures (connect refused, reset, …).
        // HTTP-level errors (non-2xx) are not retried.
        auto emitted_response_messages = false;
        co_await folly::coro::retryWithExponentialBackoff(
          config.max_retry_count, config.retry_delay, config.retry_delay * 5,
          0.0 /* no jitter */,
          [&]() -> Task<void> {
            emitted_response_messages = false;
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
            reader
              .onHeadersAsync([mq, &emitted_response_messages](
                                std::unique_ptr<proxygen::HTTPMessage> msg,
                                bool is_final,
                                bool) mutable -> folly::coro::Task<bool> {
                if (not is_final) {
                  // Ignore informational 1xx headers and wait for final
                  // response headers.
                  co_return proxygen::coro::HTTPSourceReader::Continue;
                }
                emitted_response_messages = true;
                auto status = msg->getStatusCode();
                auto hdrs = std::vector<std::pair<std::string, std::string>>{};
                msg->getHeaders().forEach([&](std::string& k, std::string& v) {
                  hdrs.emplace_back(k, v);
                });
                co_await mq->enqueue(ResponseHeader{status, std::move(hdrs)});
                co_return proxygen::coro::HTTPSourceReader::Continue;
              })
              .onBodyAsync([mq, &emitted_response_messages](
                             quic::BufQueue buf_queue,
                             bool) mutable -> folly::coro::Task<bool> {
                if (not buf_queue.empty()) {
                  emitted_response_messages = true;
                  auto iobuf = buf_queue.move();
                  iobuf->coalesce();
                  auto cptr = chunk::copy(iobuf->data(), iobuf->length());
                  co_await mq->enqueue(ResponseBody{std::move(cptr)});
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
          },
          [&](folly::exception_wrapper const& ew) {
            // Once an attempt emitted response messages, do not retry.
            // Retrying after partial output can duplicate/corrupt downstream
            // parser and pagination state.
            if (emitted_response_messages) {
              return false;
            }
            if (ew.is_compatible_with<folly::AsyncSocketException>()) {
              return true;
            }
            auto retryable_http_error = false;
            ew.with_exception([&](proxygen::coro::HTTPError const& err) {
              retryable_http_error = is_retryable_http_error(err.code);
            });
            return retryable_http_error;
          });
      }());
      if (result.hasException()) {
        co_await mq->enqueue(
          FetchError{result.exception().what().toStdString()});
      }
      co_await mq->enqueue(FetchDone{});
    }(evb, std::move(url), std::move(request), std::move(config),
                                                 std::move(mq)));
}

// ---- Operator ---------------------------------------------------------------

class FromHttp final : public Operator<void, table_slice> {
public:
  explicit FromHttp(FromHttpArgs args)
    : message_queue_{std::in_place, message_queue_capacity},
      args_{std::move(args)} {
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
    if (not co_await resolve_http_secrets(ctx, args_, resolved_url,
                                          resolved_headers_)) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    pagination_.current_url = resolved_url;
    // prepare fetch config
    auto fetch_config = make_fetch_config(args_, ctx);
    if (not fetch_config) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    fetch_config_ = std::move(*fetch_config);
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
          if (is_link_pagination(paginate_)) {
            TENZIR_ASSERT(paginate_);
            // Extract the rel=next URL for link pagination.
            pagination_.next_url
              = next_url_from_link_headers(response_->headers,
                                           pagination_.current_url,
                                           paginate_->source, ctx.dh());
          }
        } else {
          if (not args_.error_field) {
            diagnostic::error("received HTTP error status {}",
                              response_->status)
              .primary(args_.url.source)
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
          .primary(args_.url.source)
          .emit(ctx);
        pagination_.next_url = None{};
        lifecycle_ = Lifecycle::done;
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
    if (not pagination_.next_url) {
      if (auto next = next_url_from_lambda(paginate_, slice,
                                           pagination_.current_url, ctx.dh())) {
        pagination_.next_url = std::move(*next);
      }
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (pagination_.next_url) {
      // next page
      pagination_.current_url = std::move(*pagination_.next_url);
      pagination_.next_url = None{};
      pagination_.page_count += 1;
      co_await start_fetch(ctx,
                           make_paginated_request_config(resolved_headers_));
    } else {
      lifecycle_ = Lifecycle::done;
    }
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  enum class Lifecycle { running, done };

  // Starts the fetch task for the current page.
  // Requires pagination_.current_url and pagination_.page_count to be set.
  auto start_fetch(OpCtx& ctx, RequestConfig request) -> Task<void> {
    response_ = None{};
    ctx.spawn_task(fetch(evb_, proxygen::URL{pagination_.current_url},
                         std::move(request), fetch_config_, message_queue_));
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
    auto paginate_arg = d.named("paginate", &FromHttpArgs::paginate, "any");
    auto connection_timeout_arg
      = d.named("connection_timeout", &FromHttpArgs::connection_timeout);
    auto max_retry_count_arg
      = d.named("max_retry_count", &FromHttpArgs::max_retry_count);
    auto retry_delay_arg = d.named("retry_delay", &FromHttpArgs::retry_delay);
    auto encode_arg = d.named("encode", &FromHttpArgs::encode);
    auto server_arg = d.named("server", &FromHttpArgs::server);
    auto parser_arg = d.pipeline(&FromHttpArgs::parser,
                                 {{"response", &FromHttpArgs::response}});
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
      // Validate encode: requires a body and must be "json" or "form".
      if (auto encode = ctx.get(encode_arg)) {
        if (not ctx.get(body_arg)) {
          diagnostic::error("`encode` requires a `body`")
            .primary(encode->source)
            .emit(ctx);
        } else if (encode->inner != "json" and encode->inner != "form") {
          diagnostic::error("unsupported encoding: `{}`", encode->inner)
            .hint(R"(`encode` must be `"json"` or `"form"`)")
            .primary(encode->source)
            .emit(ctx);
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
      auto paginate = Option<pagination_spec>{None{}};
      if (auto paginate_expr = ctx.get(paginate_arg)) {
        auto validated
          = validate_paginate(Option<ast::expression>{*paginate_expr}, ctx);
        if (not validated) {
          return {};
        }
        paginate = std::move(*validated);
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
