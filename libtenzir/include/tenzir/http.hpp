//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/async.hpp"
#include "tenzir/blob.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/result.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/error.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace folly {
class SSLContext;
} // namespace folly

namespace proxygen::coro {
class HTTPSourceHolder;
} // namespace proxygen::coro

namespace tenzir::http {

struct header {
  std::string name;
  std::string value;
};

/// Base for HTTP messages.
struct message {
  std::string protocol;
  double version;
  std::vector<http::header> headers;
  std::string body;

  /// Retrieve a header with a case-insensitive lookup.
  [[nodiscard]] auto header(const std::string& name) -> http::header*;

  [[nodiscard]] auto header(const std::string& name) const
    -> const http::header*;
};

/// A HTTP request message.
struct request : message {
  std::string method;
  std::string uri;
};

/// A HTTP response message.
struct response : message {
  uint32_t status_code;
  std::string status_text;
};

/// A HTTPie-inspired request item.
struct request_item {
  /// Parses a request item like HTTPie.
  static auto parse(std::string_view str) -> std::optional<request_item>;

  enum item_type : uint8_t {
    file_data_json,
    data_json,
    url_param,
    file_data,
    file_form,
    data,
    header,
  };

  friend auto inspect(auto& f, request_item& x) -> bool {
    using enum_type = std::underlying_type_t<item_type>;
    return f.object(x)
      .pretty_name("tenzir.http.request_item")
      .fields(f.field("type", reinterpret_cast<enum_type&>(x.type)),
              f.field("key", x.key), f.field("value", x.value));
  }

  item_type type;
  std::string key;
  std::string value;
};

/// Applies a list of request items to a given HTTP request.
/// We mimic HTTPie's behavior in processing request items.
auto apply(std::vector<request_item> items, request& req) -> caf::error;

using HeaderPairs = std::vector<std::pair<std::string, std::string>>;

struct ResponseData {
  uint16_t status_code = 0;
  HeaderPairs headers;
  blob body;
};

struct RequestData {
  std::string method;
  std::string path;
  std::string fragment;
  std::string version;
  HeaderPairs headers;
  HeaderPairs query;
  blob body;
};

template <class T>
using HttpResult = Result<T, std::string>;

struct ClientRequestConfig {
  std::string url;
  std::string method = "GET";
  std::string body;
  std::unordered_map<std::string, std::string> headers;
  std::chrono::milliseconds connect_timeout = std::chrono::seconds{5};
  size_t response_limit = std::numeric_limits<int32_t>::max();
  std::shared_ptr<folly::SSLContext> ssl_context;
};

struct Endpoint {
  std::string host;
  uint16_t port = 0;
};

struct ResponseRoute {
  uint16_t code = 0;
  std::string content_type;
  std::string body;
};

auto normalize_http_method(std::string_view method)
  -> std::optional<std::string>;

auto parse_host_port_endpoint(std::string_view endpoint)
  -> Result<Endpoint, std::string>;

auto decode_query_string(std::string_view query) -> HeaderPairs;

auto parse_response_code(data const& value) -> std::optional<uint16_t>;

auto validate_response_map(record const& responses, diagnostic_handler& dh,
                           location source) -> failure_or<void>;

auto lookup_response(record const& responses, std::string_view path)
  -> std::optional<ResponseRoute>;

auto make_fixed_response_source(uint16_t code, std::string body,
                                std::string_view content_type = {})
  -> proxygen::coro::HTTPSourceHolder;

auto try_decompress_body(std::string_view encoding,
                         std::span<std::byte const> body,
                         diagnostic_handler& dh) -> std::optional<blob>;

auto find_header_value(HeaderPairs const& headers, std::string_view name)
  -> std::string_view;

auto make_response_record(ResponseData const& response) -> record;

auto make_request_record(RequestData const& request) -> record;

auto make_request_event(RequestData const& request) -> table_slice;

auto normalize_http_url(std::string& url, bool tls_enabled) -> void;

auto infer_tls_default(std::string_view url) -> bool;

auto next_url_from_link_headers(ResponseData const& response,
                                std::string_view request_uri,
                                std::optional<location> paginate_source,
                                diagnostic_handler& dh)
  -> std::optional<std::string>;

auto send_request(ClientRequestConfig config) -> Task<HttpResult<ResponseData>>;

auto send_request_with_retries(ClientRequestConfig config,
                               uint64_t max_retry_count,
                               std::chrono::milliseconds retry_delay)
  -> Task<HttpResult<ResponseData>>;

} // namespace tenzir::http
