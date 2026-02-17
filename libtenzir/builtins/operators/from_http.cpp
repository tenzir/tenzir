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
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <arrow/util/compression.h>
#include <boost/url/parse.hpp>
#include <boost/url/parse_query.hpp>
#include <boost/url/url.hpp>
#include <caf/net/http/method.hpp>
#include <folly/coro/Sleep.h>
#include <folly/executors/GlobalExecutor.h>
#define nsel_CONFIG_SELECT_EXPECTED 1
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <proxygen/lib/utils/URL.h>

#include <cctype>
#include <deque>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>

namespace tenzir::plugins::http {
namespace {

namespace http = caf::net::http;
namespace phttp = proxygen;
using namespace std::literals;

constexpr auto max_response_size = std::numeric_limits<int32_t>::max();
constexpr auto http_version_major = uint8_t{1};
constexpr auto http_version_minor = uint8_t{1};

auto try_decompress_body(std::string_view const encoding,
                         std::span<std::byte const> const body,
                         diagnostic_handler& dh) -> std::optional<blob> {
  if (encoding.empty()) {
    return std::nullopt;
  }
  auto const compression_type
    = arrow::util::Codec::GetCompressionType(std::string{encoding});
  if (not compression_type.ok()) {
    diagnostic::warning("invalid compression type: {}", encoding)
      .hint("must be one of `brotli`, `bz2`, `gzip`, `lz4`, `zstd`")
      .note("skipping decompression")
      .emit(dh);
    return std::nullopt;
  }
  auto out = blob{};
  out.resize(body.size_bytes() * 2);
  auto const codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  TENZIR_ASSERT(codec.ok());
  if (not codec.ValueUnsafe()) {
    return std::nullopt;
  }
  auto const decompressor = check(codec.ValueUnsafe()->MakeDecompressor());
  auto written = size_t{};
  auto read = size_t{};
  while (read != body.size_bytes()) {
    auto const result = decompressor->Decompress(
      detail::narrow<int64_t>(body.size_bytes() - read),
      reinterpret_cast<uint8_t const*>(body.data() + read),
      detail::narrow<int64_t>(out.size() - written),
      reinterpret_cast<uint8_t*>(out.data() + written));
    if (not result.ok()) {
      diagnostic::warning("failed to decompress: {}",
                          result.status().ToString())
        .note("emitting compressed body")
        .emit(dh);
      return std::nullopt;
    }
    TENZIR_ASSERT(std::cmp_less_equal(result->bytes_written, out.size()));
    written += result->bytes_written;
    read += result->bytes_read;
    if (result->need_more_output) {
      if (out.size() == out.max_size()) [[unlikely]] {
        diagnostic::error("failed to resize buffer").emit(dh);
        return std::nullopt;
      }
      if (out.size() < out.max_size() / 2) {
        out.resize(out.size() * 2);
      } else [[unlikely]] {
        out.resize(out.max_size());
      }
    }
    // In case the input contains multiple concatenated compressed streams,
    // we gracefully reset the decompressor.
    if (decompressor->IsFinished()) {
      auto const result = decompressor->Reset();
      if (not result.ok()) {
        diagnostic::warning("failed to reset decompressor: {}",
                            result.ToString())
          .note("emitting compressed body")
          .emit(dh);
        return std::nullopt;
      }
    }
  }
  TENZIR_ASSERT(written != 0);
  out.resize(written);
  return out;
}

// Splits a raw HTTP Link header value into individual link-value items.
//
// Implements a minimal subset of RFC 8288 by splitting only at top-level
// commas, i.e., commas that are not inside URI references (`<...>`) and not
// inside quoted strings.
auto split_link_header(std::string_view value)
  -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  auto start = size_t{0};
  auto in_angle = false;
  auto in_quotes = false;
  auto escaped = false;
  for (auto i = size_t{}; i < value.size(); ++i) {
    auto const c = value[i];
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

// Splits link-value parameters into semicolon-separated parts.
//
// This follows RFC 8288-style quoting semantics for delimiters by ignoring
// semicolons inside quoted strings and tracking backslash escapes.
auto split_link_params(std::string_view value)
  -> std::pair<std::vector<std::string_view>, bool> {
  auto result = std::vector<std::string_view>{};
  auto start = size_t{0};
  auto in_quotes = false;
  auto escaped = false;
  for (auto i = size_t{}; i < value.size(); ++i) {
    auto const c = value[i];
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
    auto const token_begin = index;
    while (index < value.size()
           and std::isspace(static_cast<unsigned char>(value[index])) == 0) {
      ++index;
    }
    auto const token = value.substr(token_begin, index - token_begin);
    if (not token.empty() and detail::ascii_icase_equal(token, "next")) {
      return true;
    }
  }
  return false;
}

struct NextLinkTargetResult {
  std::optional<std::string_view> target;
  bool malformed = false;
};

// Parses a single RFC 8288 link-value and extracts its `rel=next` target.
auto next_link_target(std::string_view header) -> NextLinkTargetResult {
  auto item = detail::trim(header);
  if (item.empty()) {
    return {};
  }
  if (item.front() != '<') {
    return {.malformed = true};
  }
  auto const uri_end = item.find('>');
  if (uri_end == std::string_view::npos) {
    return {.malformed = true};
  }
  auto target = item.substr(1, uri_end - 1);
  auto params = item.substr(uri_end + 1);
  auto [param_parts, ok] = split_link_params(params);
  if (not ok) {
    return {.malformed = true};
  }
  auto has_next = false;
  for (auto const part : param_parts) {
    auto const eq = part.find('=');
    auto name = detail::trim(part.substr(0, eq));
    if (name.empty()) {
      continue;
    }
    if (not detail::ascii_icase_equal(name, "rel")) {
      continue;
    }
    if (eq == std::string_view::npos) {
      continue;
    }
    auto const value = detail::trim(part.substr(eq + 1));
    if (rel_contains_next(value)) {
      has_next = true;
      break;
    }
  }
  if (has_next) {
    return {.target = target};
  }
  return {};
}

struct ProxygenResponseData {
  uint16_t status_code = 0;
  std::vector<std::pair<std::string, std::string>> headers;
  blob body;
};

template <class T>
using HttpResult = Result<T, std::string>;

auto to_proxygen_method(http::method method) -> std::optional<phttp::HTTPMethod> {
  switch (method) {
    case http::method::get:
      return phttp::HTTPMethod::GET;
    case http::method::head:
      return phttp::HTTPMethod::HEAD;
    case http::method::post:
      return phttp::HTTPMethod::POST;
    case http::method::put:
      return phttp::HTTPMethod::PUT;
    case http::method::del:
      return phttp::HTTPMethod::DELETE;
    case http::method::connect:
      return phttp::HTTPMethod::CONNECT;
    case http::method::options:
      return phttp::HTTPMethod::OPTIONS;
    case http::method::trace:
      return phttp::HTTPMethod::TRACE;
    default:
      return std::nullopt;
  }
}

auto to_chrono(duration d) -> std::chrono::milliseconds {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

auto find_header_value(
  std::vector<std::pair<std::string, std::string>> const& headers,
  std::string_view name) -> std::string_view {
  auto const it = std::ranges::find_if(headers, [&](auto const& kv) {
    return detail::ascii_icase_equal(kv.first, name);
  });
  if (it == headers.end()) {
    return {};
  }
  return it->second;
}

auto make_response_record(ProxygenResponseData const& response) -> record {
  auto headers = record{};
  for (auto const& [k, v] : response.headers) {
    headers.emplace(k, v);
  }
  return record{
    {"code", static_cast<uint64_t>(response.status_code)},
    {"headers", std::move(headers)},
  };
}

auto next_url_from_link_headers(std::optional<located<std::string>> const& paginate,
                                ProxygenResponseData const& response,
                                std::string_view request_uri,
                                diagnostic_handler& dh)
  -> std::optional<std::string> {
  auto const link_pagination = paginate and paginate->inner == "link";
  if (not link_pagination) {
    return std::nullopt;
  }
  auto const base_uri = boost::urls::parse_uri_reference(request_uri);
  if (not base_uri) {
    diagnostic::warning("failed to parse request URI for link pagination: {}",
                        base_uri.error().message())
      .primary(paginate->source)
      .note("stopping pagination")
      .emit(dh);
    return std::nullopt;
  }
  auto malformed = false;
  for (auto const& [name, value] : response.headers) {
    if (not detail::ascii_icase_equal(name, "link")) {
      continue;
    }
    for (auto const header : split_link_header(value)) {
      auto const parsed = next_link_target(header);
      if (parsed.target) {
        auto ref = boost::urls::parse_uri_reference(*parsed.target);
        if (not ref) {
          diagnostic::warning("invalid `rel=next` URL in Link header: {}",
                              ref.error().message())
            .primary(paginate->source)
            .note("stopping pagination")
            .emit(dh);
          return std::nullopt;
        }
        auto resolved = boost::urls::url{};
        if (auto result = boost::urls::resolve(*base_uri, *ref, resolved);
            not result) {
          diagnostic::warning("failed to resolve `rel=next` URL: {}",
                              result.error().message())
            .primary(paginate->source)
            .note("stopping pagination")
            .emit(dh);
          return std::nullopt;
        }
        return std::string{resolved.buffer()};
      }
      if (parsed.malformed) {
        malformed = true;
      }
    }
  }
  if (malformed) {
    diagnostic::warning("failed to parse Link header for pagination")
      .primary(paginate->source)
      .note("stopping pagination")
      .emit(dh);
  }
  return std::nullopt;
}

struct ProxygenRequestConfig {
  std::string url;
  phttp::HTTPMethod method = phttp::HTTPMethod::GET;
  std::string body;
  std::unordered_map<std::string, std::string> headers;
  std::chrono::milliseconds connect_timeout = std::chrono::seconds{5};
  size_t response_limit = size_t{max_response_size};
  std::shared_ptr<folly::SSLContext> ssl_context;
};

class ProxygenClientRequest final : public phttp::HTTPConnector::Callback,
                                      public phttp::HTTPTransactionHandler {
public:
  ProxygenClientRequest(ProxygenRequestConfig config, folly::EventBase* evb)
    : config_{std::move(config)}, evb_{evb} {
  }

  auto run() -> Task<HttpResult<ProxygenResponseData>> {
    auto url = phttp::URL{config_.url};
    if (not url.isValid() or not url.hasHost()) {
      co_return Err{fmt::format("failed to parse uri `{}`", config_.url)};
    }
    url_ = std::move(url);
    auto timeout = phttp::WheelTimerInstance{config_.connect_timeout, evb_};
    connector_ = Box<phttp::HTTPConnector>{
      std::in_place, static_cast<phttp::HTTPConnector::Callback*>(this), timeout};
    auto addr = folly::SocketAddress{url_.getHost(), url_.getPort(), true};
    if (url_.isSecure()) {
      if (not config_.ssl_context) {
        co_return Err{
          std::string{"TLS is enabled for URL but no TLS context is available"}};
      }
      (*connector_)->connectSSL(evb_, addr, config_.ssl_context, nullptr,
                                config_.connect_timeout,
                                folly::emptySocketOptionMap,
                                folly::AsyncSocket::anyAddress(), url_.getHost());
    } else {
      (*connector_)->connect(evb_, addr, config_.connect_timeout);
    }
    co_await done_.wait();
    TENZIR_ASSERT(result_);
    co_return std::move(*result_);
  }

  void connectSuccess(phttp::HTTPUpstreamSession* session) override {
    session_ = session;
    auto* txn = session_->newTransaction(
      static_cast<phttp::HTTPTransaction::Handler*>(this));
    if (not txn) {
      finish(Err{std::string{"failed to create http transaction"}});
      return;
    }
    auto request = phttp::HTTPMessage{};
    request.setMethod(config_.method);
    request.setHTTPVersion(http_version_major, http_version_minor);
    request.setURL(url_.makeRelativeURL());
    request.setSecure(url_.isSecure());
    request.getHeaders().add("Host", url_.getHostAndPort());
    for (auto const& [k, v] : config_.headers) {
      request.getHeaders().add(k, v);
    }
    txn->sendHeaders(request);
    if (not config_.body.empty()) {
      txn->sendBody(folly::IOBuf::copyBuffer(config_.body));
    }
    txn->sendEOM();
    session_->closeWhenIdle();
  }

  void connectError(folly::AsyncSocketException const& ex) override {
    finish(Err{fmt::format("cannot_connect_to_node: {}", ex.what())});
  }

  void setTransaction(phttp::HTTPTransaction* txn) noexcept override {
    txn_ = txn;
  }

  void detachTransaction() noexcept override {
    txn_ = nullptr;
  }

  void onHeadersComplete(std::unique_ptr<phttp::HTTPMessage> msg) noexcept override {
    response_.status_code = msg->getStatusCode();
    msg->getHeaders().forEach([&](std::string const& name,
                                  std::string const& value) {
      response_.headers.emplace_back(name, value);
    });
  }

  void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override {
    if (not chain) {
      return;
    }
    auto bytes = chain->computeChainDataLength();
    if (response_.body.size() + bytes > config_.response_limit) {
      finish(Err{fmt::format("response size exceeds configured limit of {} bytes",
                             config_.response_limit)});
      if (txn_) {
        txn_->sendAbort();
      }
      return;
    }
    for (auto const& range : *chain) {
      auto const* begin = reinterpret_cast<std::byte const*>(range.data());
      response_.body.insert(response_.body.end(), begin, begin + range.size());
    }
  }

  void onEOM() noexcept override {
    finish(HttpResult<ProxygenResponseData>{std::move(response_)});
  }

  void onError(phttp::HTTPException const& error) noexcept override {
    finish(Err{fmt::format("request failed: {}", error.what())});
  }

  void onTrailers(std::unique_ptr<phttp::HTTPHeaders>) noexcept override {
  }

  void onUpgrade(phttp::UpgradeProtocol) noexcept override {
  }

  void onEgressPaused() noexcept override {
  }

  void onEgressResumed() noexcept override {
  }

private:
  auto finish(HttpResult<ProxygenResponseData> result) -> void {
    if (result_) {
      return;
    }
    result_ = std::move(result);
    done_.notify_one();
  }

  ProxygenRequestConfig config_;
  folly::EventBase* evb_ = nullptr;
  phttp::URL url_;
  std::optional<Box<phttp::HTTPConnector>> connector_;
  phttp::HTTPUpstreamSession* session_ = nullptr;
  phttp::HTTPTransaction* txn_ = nullptr;
  ProxygenResponseData response_;
  std::optional<HttpResult<ProxygenResponseData>> result_;
  Notify done_;
};

struct FromHTTPExecutorArgs {
  location op = location::unknown;
  located<secret> url;
  std::optional<located<std::string>> method;
  std::optional<located<data>> body;
  std::optional<located<std::string>> encode;
  std::optional<located<record>> headers;
  std::optional<ast::field_path> error_field;
  std::optional<located<std::string>> paginate;
  located<duration> paginate_delay{0s, location::unknown};
  located<duration> connection_timeout{5s, location::unknown};
  located<uint64_t> max_retry_count{0, location::unknown};
  located<duration> retry_delay{1s, location::unknown};
  std::optional<located<data>> tls;
  std::optional<located<ir::pipeline>> parse;
  let_id response_let;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (headers) {
      for (auto const& [_, v] : headers->inner) {
        if (not is<std::string>(v) and not is<secret>(v)) {
          diagnostic::error("header values must be of type `string`")
            .primary(*headers)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    if (body) {
      TRY(match(
        body->inner,
        [](concepts::one_of<blob, std::string, record> auto const&)
          -> failure_or<void> {
          return {};
        },
        [&](auto const&) -> failure_or<void> {
          diagnostic::error("`body` must be `blob`, `record` or `string`")
            .primary(body->source)
            .emit(dh);
          return failure::promise();
        }));
    }
    if (encode) {
      if (not body) {
        diagnostic::error("encoding specified without a `body`")
          .primary(encode->source)
          .emit(dh);
        return failure::promise();
      }
      if (encode->inner != "json" and encode->inner != "form") {
        diagnostic::error("unsupported encoding: `{}`", encode->inner)
          .primary(encode->source)
          .hint("must be `json` or `form`")
          .emit(dh);
        return failure::promise();
      }
    }
    if (method and method->inner.empty()) {
      diagnostic::error("`method` must not be empty").primary(*method).emit(dh);
      return failure::promise();
    }
    if (not make_method()) {
      diagnostic::error("invalid http method: `{}`", method->inner)
        .primary(*method)
        .emit(dh);
      return failure::promise();
    }
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (paginate and paginate->inner != "link") {
      diagnostic::error("unsupported pagination mode: `{}`", paginate->inner)
        .primary(paginate->source)
        .hint("`paginate` must be `\"link\"`")
        .emit(dh);
      return failure::promise();
    }
    if (parse) {
      auto output = parse->inner.infer_type(tag_v<chunk_ptr>, dh);
      if (not output) {
        return failure::promise();
      }
      if (*output and not (*output)->is_any<void, table_slice>()) {
        diagnostic::error("pipeline must return events or be a sink")
          .primary(*parse)
          .emit(dh);
        return failure::promise();
      }
    }
    auto tls_opts = tls ? tls_options{*tls, {.tls_default = false}}
                        : tls_options{{.tls_default = false}};
    TRY(tls_opts.validate(dh));
    return {};
  }

  auto make_method() const -> std::optional<phttp::HTTPMethod> {
    auto method_name = std::string{};
    if (not method) {
      method_name = body ? "post" : "get";
    } else {
      method_name = method->inner;
    }
    auto caf_method = http::method{};
    if (not http::from_string(method_name, caf_method)) {
      return std::nullopt;
    }
    return to_proxygen_method(caf_method);
  }

  auto make_headers() const
    -> std::pair<std::unordered_map<std::string, std::string>,
                 detail::stable_map<std::string, secret>> {
    auto hdrs = std::unordered_map<std::string, std::string>{};
    auto secrets = detail::stable_map<std::string, secret>{};
    auto insert_accept_header = true;
    auto insert_content_type = body and is<record>(body->inner);
    if (headers) {
      for (auto const& [k, v] : headers->inner) {
        if (detail::ascii_icase_equal(k, "accept")) {
          insert_accept_header = false;
        }
        if (detail::ascii_icase_equal(k, "content-type")) {
          insert_content_type = false;
        }
        match(
          v,
          [&](std::string const& x) {
            hdrs.emplace(k, x);
          },
          [&](secret const& x) {
            secrets.emplace(k, x);
          },
          [](auto const&) {
            TENZIR_UNREACHABLE();
          });
      }
    }
    if (insert_content_type) {
      hdrs.emplace("Content-Type", encode and encode->inner == "form"
                                     ? "application/x-www-form-urlencoded"
                                     : "application/json");
    }
    if (insert_accept_header) {
      hdrs.emplace("Accept", "application/json, */*;q=0.5");
    }
    return std::pair{hdrs, secrets};
  }
};

struct ExecutorHTTPRequest {
  std::string url;
  std::unordered_map<std::string, std::string> headers;
  bool is_pagination = false;
};

auto normalize_http_url(std::string& url, bool tls_enabled) -> void {
  if (not url.starts_with("http://") and not url.starts_with("https://")) {
    url.insert(0, tls_enabled ? "https://" : "http://");
  } else if (tls_enabled and url.starts_with("http://")) {
    url.insert(4, "s");
  }
}

auto queue_executor_request(
  std::deque<ExecutorHTTPRequest>& queue,
  std::unordered_map<std::string, std::string> const& headers,
  std::string next_url, bool tls_enabled, location const& op,
  diagnostic_handler& dh, severity diag_severity, std::string_view note = {})
  -> bool {
  normalize_http_url(next_url, tls_enabled);
  auto url = phttp::URL{next_url};
  if (not url.isValid()) {
    if (diag_severity == severity::warning) {
      if (note.empty()) {
        diagnostic::warning("failed to parse uri: {}", next_url).primary(op).emit(dh);
      } else {
        diagnostic::warning("failed to parse uri: {}", next_url)
          .primary(op)
          .note("{}", note)
          .emit(dh);
      }
    } else {
      if (note.empty()) {
        diagnostic::error("failed to parse uri: {}", next_url).primary(op).emit(dh);
      } else {
        diagnostic::error("failed to parse uri: {}", next_url)
          .primary(op)
          .note("{}", note)
          .emit(dh);
      }
    }
    return false;
  }
  queue.push_back({std::move(next_url), headers, true});
  return true;
}

struct FromHTTPTaskEvent {
  std::optional<ExecutorHTTPRequest> request;
  std::optional<HttpResult<ProxygenResponseData>> response;
};

class FromHTTPExecutorOperator final : public Operator<void, table_slice> {
public:
  explicit FromHTTPExecutorOperator(FromHTTPExecutorArgs args)
    : args_{std::move(args)}, response_let_id_{args_.response_let} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    auto requests = std::vector<secret_request>{};
    auto [headers, secrets] = args_.make_headers();
    requests.emplace_back(make_secret_request("url", args_.url, resolved_url_, dh));
    if (not secrets.empty()) {
      auto const& loc = args_.headers->source;
      for (auto& [name, secret] : secrets) {
        auto request = secret_request{
          std::move(secret),
          loc,
          [&, name](resolved_secret_value const& value) -> failure_or<void> {
            TRY(auto str, value.utf8_view(name, loc, dh));
            headers.emplace(name, std::string{str});
            return {};
          },
        };
        requests.emplace_back(std::move(request));
      }
    }
    if (not requests.empty()) {
      auto resolved = co_await ctx.resolve_secrets(std::move(requests));
      if (not resolved) {
        done_ = true;
        co_return;
      }
    }
    if (resolved_url_.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      done_ = true;
      co_return;
    }
    tls_options_ = args_.tls ? tls_options{*args_.tls, {.tls_default = false}}
                             : tls_options{{.tls_default = false}};
    auto validate_tls
      = tls_options_.validate(resolved_url_, args_.url.source, dh);
    if (not validate_tls) {
      done_ = true;
      co_return;
    }
    auto ssl_result = tls_options_.make_folly_ssl_context(dh);
    if (not ssl_result) {
      done_ = true;
      co_return;
    }
    ssl_context_ = std::move(*ssl_result);
    tls_enabled_ = tls_options_.get_tls(nullptr).inner;
    normalize_http_url(resolved_url_, tls_enabled_);
    pending_.push_back({resolved_url_, std::move(headers), false});
    if (args_.body) {
      match(
        args_.body->inner,
        [&](blob const& x) {
          request_body_.append(reinterpret_cast<char const*>(x.data()), x.size());
        },
        [&](std::string const& x) {
          request_body_ = x;
        },
        [&](record const& x) {
          if (args_.encode and args_.encode->inner == "form") {
            request_body_ = curl::escape(flatten(x));
            return;
          }
          auto printer = json_printer{{}};
          auto it = std::back_inserter(request_body_);
          printer.print(it, x);
        },
        [](auto const&) {
          TENZIR_UNREACHABLE();
        });
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (not active_sub_ and not pending_.empty()) {
      auto request = std::move(pending_.front());
      pending_.pop_front();
      if (request.is_pagination and args_.paginate_delay.inner > duration::zero()) {
        co_await folly::coro::sleep(
          std::chrono::duration_cast<folly::HighResDuration>(
            to_chrono(args_.paginate_delay.inner)));
      }
      auto result = co_await perform_request(request);
      co_return FromHTTPTaskEvent{
        std::optional<ExecutorHTTPRequest>{std::move(request)},
        std::optional<HttpResult<ProxygenResponseData>>{std::move(result)},
      };
    }
    co_await notify_->wait();
    co_return FromHTTPTaskEvent{};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto event = std::move(result).as<FromHTTPTaskEvent>();
    if (not event.response) {
      if (not active_sub_ and pending_.empty()) {
        done_ = true;
      }
      co_return;
    }
    TENZIR_ASSERT(event.request);
    auto request = std::move(*event.request);
    auto response_result = std::move(*event.response);
    if (response_result.is_err()) {
      diagnostic::error("{}", std::move(response_result).unwrap_err())
        .primary(args_.op)
        .emit(ctx);
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      }
      co_return;
    }
    auto response = std::move(response_result).unwrap();
    if (auto const code = response.status_code; code < 200 or code > 399) {
      if (not args_.error_field) {
        diagnostic::error("received erroneous http status code: `{}`", code)
          .primary(args_.op)
          .hint("specify `error_field` to keep the event")
          .emit(ctx);
      } else {
        auto sb = series_builder{};
        std::ignore = sb.record();
        auto error = series_builder{};
        error.data(response.body);
        auto slice = assign(*args_.error_field, error.finish_assert_one_array(),
                            sb.finish_assert_one_slice(), ctx.dh());
        co_await push(std::move(slice));
      }
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      }
      co_return;
    }
    if (args_.paginate and args_.paginate->inner == "link") {
      if (auto url = next_url_from_link_headers(args_.paginate, response, request.url,
                                                ctx.dh())) {
        std::ignore = queue_executor_request(pending_, request.headers, std::move(*url),
                                             tls_enabled_, args_.op, ctx.dh(),
                                             severity::error);
      }
    }
    if (response.body.empty()) {
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      } else {
        notify_->notify_one();
      }
      co_return;
    }
    if (not args_.parse) {
      diagnostic::error("`from_http` in the new executor requires an explicit parsing pipeline")
        .primary(args_.op)
        .emit(ctx);
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      }
      co_return;
    }
    auto response_body = std::move(response.body);
    auto chunk_data = std::span<std::byte const>{response_body.data(),
                                                 response_body.size()};
    auto encoding = find_header_value(response.headers, "content-encoding");
    auto payload = chunk_ptr{};
    if (auto decompressed = try_decompress_body(encoding, chunk_data, ctx.dh())) {
      payload = chunk::make(std::move(*decompressed));
    } else {
      payload = chunk::make(std::move(response_body));
    }
    auto response_record = make_response_record(response);
    auto pipeline = args_.parse->inner;
    auto env = substitute_ctx::env_t{};
    env[response_let_id_] = std::move(response_record);
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    auto sub_result = pipeline.substitute(substitute_ctx{b_ctx, &env}, true);
    if (not sub_result) {
      co_return;
    }
    auto sub_key = next_sub_key_++;
    auto sub = co_await ctx.spawn_sub(data{sub_key}, std::move(pipeline),
                                      tag_v<chunk_ptr>);
    auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
    auto push_result = co_await open_pipeline.push(std::move(payload));
    if (push_result.is_err()) {
      done_ = true;
      co_return;
    }
    co_await open_pipeline.close();
    active_sub_ = ActiveSubContext{
      .key = sub_key,
    };
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx&)
    -> Task<void> override {
    if (not active_sub_) {
      co_return;
    }
    if (materialize(key) != data{active_sub_->key}) {
      co_return;
    }
    if (slice.rows() == 0) {
      co_return;
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&) -> Task<void> override {
    if (not active_sub_) {
      co_return;
    }
    if (materialize(key) != data{active_sub_->key}) {
      co_return;
    }
    active_sub_ = std::nullopt;
    if (pending_.empty()) {
      done_ = true;
    } else {
      notify_->notify_one();
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  struct ActiveSubContext {
    uint64_t key = 0;
  };

  auto perform_request(ExecutorHTTPRequest const& request) const
    -> Task<HttpResult<ProxygenResponseData>> {
    auto attempts = uint64_t{};
    auto max_attempts = args_.max_retry_count.inner + 1;
    auto request_method = args_.make_method();
    TENZIR_ASSERT(request_method);
    while (attempts < max_attempts) {
      auto config = ProxygenRequestConfig{
        .url = request.url,
        .method = *request_method,
        .body = request_body_,
        .headers = request.headers,
        .connect_timeout = to_chrono(args_.connection_timeout.inner),
        .response_limit = size_t{max_response_size},
        .ssl_context = ssl_context_,
      };
      auto* evb = folly::getGlobalIOExecutor()->getEventBase();
      TENZIR_ASSERT(evb);
      auto request_task = Box<ProxygenClientRequest>{std::in_place,
                                                       std::move(config), evb};
      auto result = co_await folly::coro::co_withExecutor(evb, request_task->run());
      if (not result.is_err()) {
        co_return result;
      }
      ++attempts;
      if (attempts >= max_attempts) {
        co_return result;
      }
      if (args_.retry_delay.inner > duration::zero()) {
        co_await folly::coro::sleep(
          std::chrono::duration_cast<folly::HighResDuration>(
            to_chrono(args_.retry_delay.inner)));
      }
    }
    co_return Err{std::string{"request failed"}};
  }

  FromHTTPExecutorArgs args_;
  mutable std::string resolved_url_;
  mutable std::string request_body_;
  mutable std::shared_ptr<folly::SSLContext> ssl_context_;
  mutable tls_options tls_options_{{.tls_default = false}};
  mutable std::deque<ExecutorHTTPRequest> pending_;
  mutable std::optional<ActiveSubContext> active_sub_;
  let_id response_let_id_;
  uint64_t next_sub_key_ = 0;
  mutable std::shared_ptr<Notify> notify_ = std::make_shared<Notify>();
  mutable bool tls_enabled_ = false;
  bool done_ = false;
};

} // namespace

struct FromHTTP final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.from_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromHTTPExecutorArgs, FromHTTPExecutorOperator>{};
    d.operator_location(&FromHTTPExecutorArgs::op);
    auto url = d.positional("url", &FromHTTPExecutorArgs::url);
    auto method = d.named("method", &FromHTTPExecutorArgs::method);
    auto body = d.named("body", &FromHTTPExecutorArgs::body);
    auto encode = d.named("encode", &FromHTTPExecutorArgs::encode);
    auto headers = d.named("headers", &FromHTTPExecutorArgs::headers);
    auto error_field
      = d.named("error_field", &FromHTTPExecutorArgs::error_field);
    auto paginate = d.named("paginate", &FromHTTPExecutorArgs::paginate);
    auto paginate_delay
      = d.named_optional("paginate_delay", &FromHTTPExecutorArgs::paginate_delay);
    auto connection_timeout = d.named_optional(
      "connection_timeout", &FromHTTPExecutorArgs::connection_timeout);
    auto max_retry_count
      = d.named_optional("max_retry_count", &FromHTTPExecutorArgs::max_retry_count);
    auto retry_delay
      = d.named_optional("retry_delay", &FromHTTPExecutorArgs::retry_delay);
    auto tls = d.named("tls", &FromHTTPExecutorArgs::tls);
    auto parse
      = d.pipeline(&FromHTTPExecutorArgs::parse,
                   {{"response", &FromHTTPExecutorArgs::response_let}});
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto args = FromHTTPExecutorArgs{};
      args.op = ctx.get_location(url).value_or(location::unknown);
      if (auto x = ctx.get(url)) {
        args.url = *x;
      }
      if (auto x = ctx.get(method)) {
        args.method = *x;
      }
      if (auto x = ctx.get(body)) {
        args.body = *x;
      }
      if (auto x = ctx.get(encode)) {
        args.encode = *x;
      }
      if (auto x = ctx.get(headers)) {
        args.headers = *x;
      }
      if (auto x = ctx.get(error_field)) {
        args.error_field = *x;
      }
      if (auto x = ctx.get(paginate)) {
        args.paginate = *x;
      }
      if (auto x = ctx.get(paginate_delay)) {
        args.paginate_delay = *x;
      }
      if (auto x = ctx.get(connection_timeout)) {
        args.connection_timeout = *x;
      }
      if (auto x = ctx.get(max_retry_count)) {
        args.max_retry_count = *x;
      }
      if (auto x = ctx.get(retry_delay)) {
        args.retry_delay = *x;
      }
      if (auto x = ctx.get(tls)) {
        args.tls = *x;
      }
      if (auto x = ctx.get(parse)) {
        args.parse = *x;
      }
      std::ignore = args.validate(ctx);
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::FromHTTP)
