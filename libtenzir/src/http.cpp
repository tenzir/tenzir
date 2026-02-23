//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http.hpp"

#include "tenzir/async/notify.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/series_builder.hpp"

#include <arrow/util/compression.h>
#include <boost/url/parse.hpp>
#include <boost/url/parse_query.hpp>
#include <boost/url/url.hpp>
#include <folly/coro/Sleep.h>
#include <folly/executors/GlobalExecutor.h>
#define nsel_CONFIG_SELECT_EXPECTED 1
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <proxygen/lib/utils/URL.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <deque>
#include <ranges>
#include <string_view>

namespace tenzir::http {

namespace {

constexpr auto http_version_major = uint8_t{1};
constexpr auto http_version_minor = uint8_t{1};

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

enum class HttpMethod : uint8_t {
  get,
  head,
  post,
  put,
  del,
  connect,
  options,
  trace,
};

auto parse_http_method(std::string_view method) -> std::optional<HttpMethod> {
  if (detail::ascii_icase_equal(method, "get")) {
    return HttpMethod::get;
  }
  if (detail::ascii_icase_equal(method, "head")) {
    return HttpMethod::head;
  }
  if (detail::ascii_icase_equal(method, "post")) {
    return HttpMethod::post;
  }
  if (detail::ascii_icase_equal(method, "put")) {
    return HttpMethod::put;
  }
  if (detail::ascii_icase_equal(method, "delete")
      or detail::ascii_icase_equal(method, "del")) {
    return HttpMethod::del;
  }
  if (detail::ascii_icase_equal(method, "connect")) {
    return HttpMethod::connect;
  }
  if (detail::ascii_icase_equal(method, "options")) {
    return HttpMethod::options;
  }
  if (detail::ascii_icase_equal(method, "trace")) {
    return HttpMethod::trace;
  }
  return std::nullopt;
}

auto to_rfc_http_method(HttpMethod method) -> std::string_view {
  switch (method) {
    case HttpMethod::get:
      return "GET";
    case HttpMethod::head:
      return "HEAD";
    case HttpMethod::post:
      return "POST";
    case HttpMethod::put:
      return "PUT";
    case HttpMethod::del:
      return "DELETE";
    case HttpMethod::connect:
      return "CONNECT";
    case HttpMethod::options:
      return "OPTIONS";
    case HttpMethod::trace:
      return "TRACE";
  }
  TENZIR_UNREACHABLE();
}

auto to_proxygen_method(HttpMethod method) -> proxygen::HTTPMethod {
  switch (method) {
    case HttpMethod::get:
      return proxygen::HTTPMethod::GET;
    case HttpMethod::head:
      return proxygen::HTTPMethod::HEAD;
    case HttpMethod::post:
      return proxygen::HTTPMethod::POST;
    case HttpMethod::put:
      return proxygen::HTTPMethod::PUT;
    case HttpMethod::del:
      return proxygen::HTTPMethod::DELETE;
    case HttpMethod::connect:
      return proxygen::HTTPMethod::CONNECT;
    case HttpMethod::options:
      return proxygen::HTTPMethod::OPTIONS;
    case HttpMethod::trace:
      return proxygen::HTTPMethod::TRACE;
  }
  TENZIR_UNREACHABLE();
}

class ProxygenClientRequest final : public proxygen::HTTPConnector::Callback,
                                    public proxygen::HTTPTransactionHandler {
public:
  ProxygenClientRequest(ClientRequestConfig config, folly::EventBase* evb)
    : config_{std::move(config)}, evb_{evb} {
  }

  auto run() -> Task<HttpResult<ResponseData>> {
    auto url = proxygen::URL{config_.url};
    if (not url.isValid() or not url.hasHost()) {
      co_return Err{fmt::format("failed to parse uri `{}`", config_.url)};
    }
    auto method = parse_http_method(config_.method);
    if (not method) {
      co_return Err{
        fmt::format("unsupported http method: `{}`", config_.method)};
    }
    method_ = to_proxygen_method(*method);
    url_ = std::move(url);
    auto timeout = proxygen::WheelTimerInstance{config_.connect_timeout, evb_};
    connector_ = Box<proxygen::HTTPConnector>{
      std::in_place, static_cast<proxygen::HTTPConnector::Callback*>(this),
      timeout};
    auto addr = folly::SocketAddress{url_.getHost(), url_.getPort(), true};
    if (url_.isSecure()) {
      if (not config_.ssl_context) {
        co_return Err{std::string{"TLS is enabled for URL but no TLS context "
                                  "is available"}};
      }
      (*connector_)
        ->connectSSL(evb_, addr, config_.ssl_context, nullptr,
                     config_.connect_timeout, folly::emptySocketOptionMap,
                     folly::AsyncSocket::anyAddress(), url_.getHost());
    } else {
      (*connector_)->connect(evb_, addr, config_.connect_timeout);
    }
    co_await done_.wait();
    TENZIR_ASSERT(result_);
    co_return std::move(*result_);
  }

  void connectSuccess(proxygen::HTTPUpstreamSession* session) override {
    session_ = session;
    auto* txn = session_->newTransaction(
      static_cast<proxygen::HTTPTransaction::Handler*>(this));
    if (not txn) {
      finish(Err{std::string{"failed to create http transaction"}});
      return;
    }
    auto request = proxygen::HTTPMessage{};
    request.setMethod(method_);
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
    auto message = std::string{ex.what()};
    // AsyncSocketException details include platform-specific errno values.
    // Strip that suffix to keep diagnostics stable across environments.
    if (auto pos = message.find(", errno = "); pos != std::string::npos) {
      message.erase(pos);
    }
    finish(Err{fmt::format("cannot_connect_to_node: {}", message)});
  }

  void setTransaction(proxygen::HTTPTransaction* txn) noexcept override {
    txn_ = txn;
  }

  void detachTransaction() noexcept override {
    txn_ = nullptr;
  }

  void onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override {
    response_.status_code = msg->getStatusCode();
    msg->getHeaders().forEach(
      [&](std::string const& name, std::string const& value) {
        response_.headers.emplace_back(name, value);
      });
  }

  void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override {
    if (not chain) {
      return;
    }
    auto bytes = chain->computeChainDataLength();
    if (response_.body.size() + bytes > config_.response_limit) {
      finish(
        Err{fmt::format("response size exceeds configured limit of {} bytes",
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
    finish(HttpResult<ResponseData>{std::move(response_)});
  }

  void onError(proxygen::HTTPException const& error) noexcept override {
    finish(Err{fmt::format("request failed: {}", error.what())});
  }

  void onTrailers(std::unique_ptr<proxygen::HTTPHeaders>) noexcept override {
  }

  void onUpgrade(proxygen::UpgradeProtocol) noexcept override {
  }

  void onEgressPaused() noexcept override {
  }

  void onEgressResumed() noexcept override {
  }

private:
  auto finish(HttpResult<ResponseData> result) -> void {
    if (result_) {
      return;
    }
    result_ = std::move(result);
    done_.notify_one();
  }

  ClientRequestConfig config_;
  folly::EventBase* evb_ = nullptr;
  proxygen::HTTPMethod method_ = proxygen::HTTPMethod::GET;
  proxygen::URL url_;
  std::optional<Box<proxygen::HTTPConnector>> connector_;
  proxygen::HTTPUpstreamSession* session_ = nullptr;
  proxygen::HTTPTransaction* txn_ = nullptr;
  ResponseData response_;
  std::optional<HttpResult<ResponseData>> result_;
  Notify done_;
};

} // namespace

auto message::header(const std::string& name) -> struct header* {
  auto pred = [&](auto& x) -> bool {
    if (x.name.size() != name.size()) {
      return false;
    }
    for (auto i = 0u; i < name.size(); ++i) {
      if (::toupper(x.name[i]) != ::toupper(name[i])) {
        return false;
      }
    }
    return true;
  };
  auto i = std::find_if(headers.begin(), headers.end(), pred);
  return i == headers.end() ? nullptr : &*i;
}

auto message::header(const std::string& name) const -> const struct header* {
  // We use a const_cast to avoid duplicating logic.
  auto* self = const_cast<message*>(this);
  return self->header(name);
}

auto request_item::parse(std::string_view str) -> std::optional<request_item> {
  auto is_valid_header_name = [](std::string_view name) {
    for (char c : name) {
      if (std::isalnum(static_cast<unsigned char>(c)) == 0 and c != '-'
          and c != '_') {
        return false;
      }
    }
    return true;
  };
  auto xs = detail::split_escaped(str, ":=@", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = file_data_json, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, ":=", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = data_json, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, ":", "\\", 1);
  if (xs.size() == 2 and is_valid_header_name(xs[0])) {
    return request_item{.type = header, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "==", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = url_param, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "=@", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = file_data, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "@", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = file_form, .key = xs[0], .value = xs[1]};
  }
  xs = detail::split_escaped(str, "=", "\\", 1);
  if (xs.size() == 2) {
    return request_item{.type = data, .key = xs[0], .value = xs[1]};
  }
  return {};
}

auto apply(std::vector<request_item> items, request& req) -> caf::error {
  auto body = record{};
  for (auto& item : items) {
    switch (item.type) {
      case request_item::header: {
        req.headers.emplace_back(std::move(item.key), std::move(item.value));
        break;
      }
      case request_item::data: {
        if (req.method.empty()) {
          req.method = "POST";
        }
        body.emplace(std::move(item.key), std::move(item.value));
        break;
      }
      case request_item::data_json: {
        if (req.method.empty()) {
          req.method = "POST";
        }
        auto data = from_json(item.value);
        if (not data) {
          return data.error();
        }
        body.emplace(std::move(item.key), std::move(*data));
        break;
      }
      case request_item::url_param: {
        auto pos = req.uri.find('?');
        if (pos == std::string::npos) {
          req.uri += '?';
        } else if (pos + 1 != req.uri.size()) {
          req.uri += '&';
        }
        req.uri += fmt::format("{}={}", curl::escape(item.key),
                               curl::escape(item.value));
        break;
      }
      default:
        return caf::make_error(ec::unimplemented, "unsupported item type");
    }
  }
  auto json_encode = [](const auto& x) {
    auto result = to_json(x, {.oneline = true});
    TENZIR_ASSERT(result);
    return std::move(*result);
  };
  auto url_encode = [](const auto& x) {
    return curl::escape(flatten(x));
  };

  // We assemble an Accept header as we go, unless we have one already.
  auto accept = std::optional<std::vector<std::string>>{};
  if (req.header("Accept") == nullptr) {
    accept = {"*/*"};
  }
  // If the user provided any request body data, we default to JSON encoding.
  // The user can override this behavior by setting a Content-Type header.
  const auto* content_type_header = req.header("Content-Type");
  if (content_type_header != nullptr
      and not content_type_header->value.empty()) {
    // Encode request body based on provided Content-Type header value.
    const auto& content_type = content_type_header->value;
    if (content_type.starts_with("application/x-www-form-urlencoded")) {
      if (not body.empty()) {
        req.body = url_encode(body);
        TENZIR_DEBUG("urlencoded request body: {}", req.body);
      }
    } else if (content_type.starts_with("application/json")) {
      if (not body.empty()) {
        req.body = json_encode(body);
        if (accept) {
          accept->insert(accept->begin(), "application/json");
        }
        TENZIR_DEBUG("JSON-encoded request body: {}", req.body);
      }
    } else {
      return caf::make_error(ec::parse_error,
                             fmt::format("cannot encode HTTP request body "
                                         "with Content-Type: {}",
                                         content_type));
    }
  } else if (not body.empty()) {
    // Without a Content-Type, we assume JSON.
    req.body = json_encode(body);
    req.headers.emplace_back("Content-Type", "application/json");
    if (accept) {
      accept->insert(accept->begin(), "application/json");
    }
  }
  // Add an Accept header unless we have one already.
  if (accept) {
    auto value = fmt::format("{}", fmt::join(*accept, ", "));
    req.headers.emplace_back("Accept", std::move(value));
  }
  return {};
}

auto normalize_http_method(std::string_view method)
  -> std::optional<std::string> {
  auto parsed = parse_http_method(method);
  if (not parsed) {
    return std::nullopt;
  }
  return std::string{to_rfc_http_method(*parsed)};
}

auto parse_host_port_endpoint(std::string_view endpoint)
  -> Result<Endpoint, std::string> {
  auto const colon = endpoint.rfind(':');
  if (colon == 0 or colon == std::string_view::npos) {
    return Err{std::string{"`url` must have the form `<host>:<port>`"}};
  }
  auto port = uint16_t{};
  auto const* begin = endpoint.data() + colon + 1;
  auto const* end = endpoint.data() + endpoint.size();
  auto [ptr, ec] = std::from_chars(begin, end, port);
  if (ec != std::errc{} or ptr != end) {
    return Err{std::string{"`url` must have the form `<host>:<port>`"}};
  }
  return Endpoint{
    .host = std::string{endpoint.substr(0, colon)},
    .port = port,
  };
}

auto decode_query_string(std::string_view query) -> HeaderPairs {
  auto result = HeaderPairs{};
  if (query.empty()) {
    return result;
  }
  auto parsed = boost::urls::parse_query(query);
  if (not parsed) {
    return result;
  }
  for (auto const& param : *parsed) {
    result.emplace_back(std::string{param.key}, std::string{param.value});
  }
  return result;
}

auto parse_response_code(data const& value) -> std::optional<uint16_t> {
  if (auto x = try_as<uint64_t>(value)) {
    if (*x <= std::numeric_limits<uint16_t>::max()) {
      return detail::narrow<uint16_t>(*x);
    }
    return std::nullopt;
  }
  if (auto x = try_as<int64_t>(value)) {
    if (*x >= 0 and *x <= std::numeric_limits<uint16_t>::max()) {
      return detail::narrow<uint16_t>(*x);
    }
    return std::nullopt;
  }
  return std::nullopt;
}

auto validate_response_map(record const& responses, diagnostic_handler& dh,
                           location source) -> failure_or<void> {
  if (responses.empty()) {
    diagnostic::error("`responses` must not be empty").primary(source).emit(dh);
    return failure::promise();
  }
  for (auto const& [_, value] : responses) {
    auto rec = try_as<record>(value);
    if (not rec) {
      diagnostic::error("field must be `record`").primary(source).emit(dh);
      return failure::promise();
    }
    if (rec->find("code") == rec->end()
        or rec->find("content_type") == rec->end()
        or rec->find("body") == rec->end()) {
      diagnostic::error(
        "`responses` record must contain `code`, `content_type`, `body`")
        .primary(source)
        .emit(dh);
      return failure::promise();
    }
    if (not parse_response_code(rec->at("code"))) {
      diagnostic::error(
        "`responses` field `code` must be an integer between 0 and 65535")
        .primary(source)
        .emit(dh);
      return failure::promise();
    }
    if (not is<std::string>(rec->at("content_type"))) {
      diagnostic::error("`responses` field `content_type` must be a string")
        .primary(source)
        .emit(dh);
      return failure::promise();
    }
    if (not is<std::string>(rec->at("body"))) {
      diagnostic::error("`responses` field `body` must be a string")
        .primary(source)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

auto lookup_response(record const& responses, std::string_view path)
  -> std::optional<ResponseRoute> {
  auto it = responses.find(std::string{path});
  if (it == responses.end()) {
    return std::nullopt;
  }
  auto rec = try_as<record>(it->second);
  if (not rec) {
    return std::nullopt;
  }
  auto code_it = rec->find("code");
  if (code_it == rec->end()) {
    return std::nullopt;
  }
  auto code = parse_response_code(code_it->second);
  if (not code) {
    return std::nullopt;
  }
  auto content_type_it = rec->find("content_type");
  if (content_type_it == rec->end()) {
    return std::nullopt;
  }
  auto content_type = try_as<std::string>(content_type_it->second);
  if (not content_type) {
    return std::nullopt;
  }
  auto body_it = rec->find("body");
  if (body_it == rec->end()) {
    return std::nullopt;
  }
  auto body = try_as<std::string>(body_it->second);
  if (not body) {
    return std::nullopt;
  }
  return ResponseRoute{
    .code = *code,
    .content_type = *content_type,
    .body = *body,
  };
}

auto make_fixed_response_source(uint16_t code, std::string body,
                                std::string_view content_type)
  -> proxygen::coro::HTTPSourceHolder {
  auto source
    = proxygen::coro::HTTPFixedSource::makeFixedResponse(code, std::move(body));
  if (not content_type.empty()) {
    source->msg_->getHeaders().set("Content-Type", std::string{content_type});
  }
  return proxygen::coro::HTTPSourceHolder{source};
}

auto try_decompress_body(std::string_view encoding,
                         std::span<std::byte const> body,
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
    if (decompressor->IsFinished()) {
      auto const reset = decompressor->Reset();
      if (not reset.ok()) {
        diagnostic::warning("failed to reset decompressor: {}",
                            reset.ToString())
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

auto find_header_value(HeaderPairs const& headers, std::string_view name)
  -> std::string_view {
  auto const it = std::ranges::find_if(headers, [&](auto const& kv) {
    return detail::ascii_icase_equal(kv.first, name);
  });
  if (it == headers.end()) {
    return {};
  }
  return it->second;
}

auto make_response_record(ResponseData const& response) -> record {
  auto headers = record{};
  for (auto const& [k, v] : response.headers) {
    headers.emplace(k, v);
  }
  return record{
    {"code", static_cast<uint64_t>(response.status_code)},
    {"headers", std::move(headers)},
  };
}

auto make_request_record(RequestData const& request) -> record {
  auto headers = record{};
  for (auto const& [k, v] : request.headers) {
    headers.emplace(k, v);
  }
  auto query = record{};
  for (auto const& [k, v] : request.query) {
    query.emplace(k, v);
  }
  return record{
    {"headers", std::move(headers)}, {"query", std::move(query)},
    {"path", request.path},          {"fragment", request.fragment},
    {"method", request.method},      {"version", request.version},
    {"body", request.body},
  };
}

auto make_request_event(RequestData const& request) -> table_slice {
  auto sb = series_builder{};
  sb.data(make_request_record(request));
  return sb.finish_assert_one_slice();
}

auto normalize_http_url(std::string& url, bool tls_enabled) -> void {
  if (not url.starts_with("http://") and not url.starts_with("https://")) {
    url.insert(0, tls_enabled ? "https://" : "http://");
  } else if (tls_enabled and url.starts_with("http://")) {
    url.insert(4, "s");
  }
}

auto infer_tls_default(std::string_view url) -> bool {
  return url.starts_with("https://");
}

auto next_url_from_link_headers(ResponseData const& response,
                                std::string_view request_uri,
                                std::optional<location> paginate_source,
                                diagnostic_handler& dh)
  -> std::optional<std::string> {
  auto const base_uri = boost::urls::parse_uri_reference(request_uri);
  if (not base_uri) {
    if (paginate_source) {
      diagnostic::warning("failed to parse request URI for link pagination: {}",
                          base_uri.error().message())
        .primary(*paginate_source, "")
        .note("stopping pagination")
        .emit(dh);
    } else {
      diagnostic::warning("failed to parse request URI for link pagination: {}",
                          base_uri.error().message())
        .note("stopping pagination")
        .emit(dh);
    }
    return std::nullopt;
  }
  auto malformed = false;
  for (auto const& [name, value] : response.headers) {
    if (not detail::ascii_icase_equal(name, "link")) {
      continue;
    }
    for (auto header : split_link_header(value)) {
      auto const parsed = next_link_target(header);
      if (parsed.target) {
        auto ref = boost::urls::parse_uri_reference(*parsed.target);
        if (not ref) {
          if (paginate_source) {
            diagnostic::warning("invalid `rel=next` URL in Link header: {}",
                                ref.error().message())
              .primary(*paginate_source, "")
              .note("stopping pagination")
              .emit(dh);
          } else {
            diagnostic::warning("invalid `rel=next` URL in Link header: {}",
                                ref.error().message())
              .note("stopping pagination")
              .emit(dh);
          }
          return std::nullopt;
        }
        auto resolved = boost::urls::url{};
        if (auto result = boost::urls::resolve(*base_uri, *ref, resolved);
            not result) {
          if (paginate_source) {
            diagnostic::warning("failed to resolve `rel=next` URL: {}",
                                result.error().message())
              .primary(*paginate_source, "")
              .note("stopping pagination")
              .emit(dh);
          } else {
            diagnostic::warning("failed to resolve `rel=next` URL: {}",
                                result.error().message())
              .note("stopping pagination")
              .emit(dh);
          }
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
    if (paginate_source) {
      diagnostic::warning("failed to parse Link header for pagination")
        .primary(*paginate_source, "")
        .note("stopping pagination")
        .emit(dh);
    } else {
      diagnostic::warning("failed to parse Link header for pagination")
        .note("stopping pagination")
        .emit(dh);
    }
  }
  return std::nullopt;
}

auto send_request(ClientRequestConfig config)
  -> Task<HttpResult<ResponseData>> {
  auto* evb = folly::getGlobalIOExecutor()->getEventBase();
  TENZIR_ASSERT(evb);
  auto request_task
    = Box<ProxygenClientRequest>{std::in_place, std::move(config), evb};
  co_return co_await folly::coro::co_withExecutor(evb, request_task->run());
}

auto send_request_with_retries(ClientRequestConfig config,
                               uint64_t max_retry_count,
                               std::chrono::milliseconds retry_delay)
  -> Task<HttpResult<ResponseData>> {
  auto attempts = uint64_t{};
  auto max_attempts = max_retry_count + 1;
  while (attempts < max_attempts) {
    auto result = co_await send_request(config);
    if (not result.is_err()) {
      co_return result;
    }
    ++attempts;
    if (attempts >= max_attempts) {
      co_return result;
    }
    if (retry_delay > std::chrono::milliseconds::zero()) {
      co_await folly::coro::sleep(
        std::chrono::duration_cast<folly::HighResDuration>(retry_delay));
    }
  }
  co_return Err{std::string{"request failed"}};
}

} // namespace tenzir::http
