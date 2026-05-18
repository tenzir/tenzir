//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/blob.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/simdjson_buffer.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tls_options.hpp"

#include <arrow/util/compression.h>
#include <caf/error.hpp>
#include <caf/fwd.hpp>
#include <folly/io/async/SSLContext.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace proxygen::coro {
enum class HTTPErrorCode : uint16_t;
} // namespace proxygen::coro

namespace tenzir::http {

auto is_retryable_http_error(proxygen::coro::HTTPErrorCode code) -> bool;

auto is_retryable_http_status(uint16_t status_code) -> bool;

auto parse_retry_after(std::string_view value) -> Option<std::chrono::seconds>;

auto retry_delay_for_attempt(std::chrono::milliseconds retry_delay,
                             uint32_t attempt,
                             Option<std::chrono::seconds> retry_after)
  -> std::chrono::milliseconds;

inline constexpr auto default_timeout = std::chrono::seconds{90};
inline constexpr auto default_connection_timeout = std::chrono::seconds{5};
inline constexpr auto default_max_retry_count = 5;
inline constexpr auto default_retry_delay = std::chrono::seconds{1};

auto add_default_url_scheme(std::string& url, bool tls_enabled) -> void;

auto is_tls_enabled(Option<located<data>> const& tls,
                    tls_options::options options,
                    std::string_view url_when_missing) -> bool;

auto normalize_url_and_tls(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           const caf::actor_system_config* cfg = nullptr,
                           tls_options::options options = {.is_server = false})
  -> failure_or<bool>;

auto make_http_pool_config(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           std::chrono::milliseconds request_timeout,
                           const caf::actor_system_config* cfg = nullptr,
                           tls_options::options options = {.is_server = false})
  -> failure_or<HttpPoolConfig>;

struct Header {
  std::string name;
  std::string value;
};

auto make_header_secret_requests(Option<located<data>> const& headers,
                                 std::vector<Header>& resolved_headers,
                                 diagnostic_handler& dh)
  -> std::vector<secret_request>;

/// Finds the first header by case-insensitive name.
auto find_header(std::span<Header const> headers, std::string_view name)
  -> Option<std::string>;

/// Removes all headers matching the case-insensitive name.
auto erase_header(std::vector<Header>& headers, std::string_view name) -> void;

/// Replaces all headers matching the case-insensitive name with one value.
auto set_header(std::vector<Header>& headers, std::string name,
                std::string value) -> void;

/// Base for HTTP messages.
struct Message {
  std::string protocol;
  double version;
  std::vector<http::Header> headers;
  std::string body;

  /// Retrieve a header with a case-insensitive lookup.
  [[nodiscard]] auto header(const std::string& name) -> http::Header*;

  [[nodiscard]] auto header(const std::string& name) const
    -> const http::Header*;
};

/// A HTTP request message.
struct Request : Message {
  std::string method;
  std::string uri;
};

/// A HTTP response message.
struct Response : Message {
  uint32_t status_code;
  std::string status_text;
};

/// A HTTPie-inspired request item.
struct RequestItem {
  /// Parses a request item like HTTPie.
  static auto parse(std::string_view str) -> Option<RequestItem>;

  enum ItemType : uint8_t {
    file_data_json,
    data_json,
    url_param,
    file_data,
    file_form,
    data,
    header,
  };

  friend auto inspect(auto& f, RequestItem& x) -> bool {
    using EnumType = std::underlying_type_t<ItemType>;
    return f.object(x)
      .pretty_name("tenzir.http.RequestItem")
      .fields(f.field("type", reinterpret_cast<EnumType&>(x.type)),
              f.field("key", x.key), f.field("value", x.value));
  }

  ItemType type;
  std::string key;
  std::string value;
};

/// Applies a list of request items to a given HTTP request.
/// We mimic HTTPie's behavior in processing request items.
auto apply(std::vector<RequestItem> items, Request& req) -> caf::error;

struct EncodedRequestBody {
  std::string body;
  Option<std::string> content_encoding = None{};
};

/// Compresses a request body for use with Content-Encoding.
///
/// Returns the original body without a Content-Encoding value and emits a
/// warning if the encoding is unsupported or compression fails.
auto compress_request_body(std::string body, std::string_view encoding,
                           diagnostic_handler& dh,
                           location loc = location::unknown)
  -> EncodedRequestBody;

/// Adds Content-Length and, when present, Content-Encoding headers.
auto add_request_body_headers(std::map<std::string, std::string>& headers,
                              EncodedRequestBody const& body) -> void;

/// Creates a streaming decompressor for the given Content-Encoding value.
/// Emits a warning and returns None for unknown or unsupported encodings.
auto make_decompressor(std::string_view encoding, diagnostic_handler& dh)
  -> Option<std::shared_ptr<arrow::util::Decompressor>>;

/// Decompresses one chunk using a persistent streaming decompressor.
/// Handles concatenated compressed streams via IsFinished/Reset.
/// Returns None and emits a warning on failure, or when the decompressed
/// output would exceed max_output_size bytes.
auto decompress_chunk(arrow::util::Decompressor& decompressor,
                      std::span<std::byte const> input, diagnostic_handler& dh,
                      size_t max_output_size
                      = std::numeric_limits<size_t>::max())
  -> Result<blob, uint16_t>;

/// Like `decompress_chunk`, but keeps simdjson padding available on the
/// returned buffer so callers can parse directly without another full copy.
auto decompress_chunk_simdjson(arrow::util::Decompressor& decompressor,
                               std::span<std::byte const> input,
                               diagnostic_handler& dh,
                               size_t max_output_size
                               = std::numeric_limits<size_t>::max())
  -> Result<SimdjsonPaddedBuffer, uint16_t>;

enum class PaginationMode {
  link,
  odata,
};

auto parse_pagination_mode(std::string_view mode) -> Option<PaginationMode>;

/// Scans all Link response headers and returns the first resolved `rel=next`
/// URL, or None if no such link is present. Emits a warning on malformed
/// headers.
auto next_url_from_link_headers(
  std::vector<Header> const& response_headers,
  std::string const& base_url, location paginate_loc, diagnostic_handler& dh)
  -> Option<std::string>;

struct OdataPage {
  Option<std::string> next_url;
  std::vector<table_slice> events;
};

/// Extracts events and the opaque next URL from an OData collection envelope.
///
/// The input must represent a single top-level object with a `value` array.
/// Each object in `value` becomes one output event. Top-level `@odata.*` fields
/// on emitted objects are omitted while nested annotations are preserved.
auto extract_odata_page(table_slice const& slice, location paginate_loc,
                        diagnostic_handler& dh) -> failure_or<OdataPage>;

} // namespace tenzir::http
