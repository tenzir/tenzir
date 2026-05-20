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
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/simdjson_buffer.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tls_options.hpp"

#include <arrow/util/compression.h>
#include <caf/error.hpp>
#include <caf/fwd.hpp>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace proxygen {
class HTTPMessage;
namespace coro {
enum class HTTPErrorCode : uint16_t;
} // namespace coro
} // namespace proxygen

namespace tenzir {

struct HttpPoolConfig;

} // namespace tenzir

namespace tenzir::http {

/// Returns whether a Proxygen transport error is worth retrying.
auto is_retryable_http_error(proxygen::coro::HTTPErrorCode code) -> bool;

/// Returns whether an HTTP status code commonly represents a transient failure.
auto is_retryable_http_status(uint16_t status_code) -> bool;

/// Parses the `Retry-After` response header.
///
/// Supports both delta-seconds and HTTP-date values. Date values in the past
/// become zero seconds.
auto parse_retry_after(std::string_view value) -> Option<std::chrono::seconds>;

/// Computes the retry sleep for an attempt.
///
/// A present `Retry-After` value takes precedence. Otherwise the result is the
/// base delay scaled by the one-based attempt number.
auto retry_delay_for_attempt(std::chrono::milliseconds retry_delay,
                             uint32_t attempt,
                             Option<std::chrono::seconds> retry_after)
  -> std::chrono::milliseconds;

/// Default request timeout used by HTTP operators.
inline constexpr auto default_timeout = std::chrono::seconds{90};
/// Default connection timeout used by `HttpPool`.
inline constexpr auto default_connection_timeout = std::chrono::seconds{5};
/// Default number of retries for retry-aware HTTP operators.
inline constexpr auto default_max_retry_count = 5;
/// Default base retry delay used when no `Retry-After` header is present.
inline constexpr auto default_retry_delay = std::chrono::seconds{1};

/// One HTTP header field.
///
/// Header names are matched case-insensitively by the helper functions below.
/// The vector representation intentionally preserves order and duplicate
/// fields because some HTTP headers are valid more than once.
struct Header {
  std::string name;
  std::string value;
};

/// Result of a completed HTTP request.
///
/// Transport failures are represented by the surrounding `Result`; this type is
/// used when an HTTP response was received, including non-2xx status codes.
struct Response {
  uint16_t status_code = 0;
  std::vector<Header> headers;
  std::string body;

  auto is_status_success() const -> bool {
    return status_code >= 200 and status_code < 300;
  }
};

/// Ensures default CA certificate paths are configured for proxygen.
auto ensure_default_ca_paths() -> void;

/// Prepends `http://` or `https://` to a URL that has no explicit scheme.
auto add_default_url_scheme(std::string& url, bool tls_enabled) -> void;

/// Determines whether a request should use TLS from an optional user setting.
///
/// When `tls` is absent, the URL scheme and TLS option defaults decide.
auto is_tls_enabled(Option<located<data>> const& tls,
                    tls_options::options options,
                    std::string_view url_when_missing) -> bool;

/// Normalizes a URL and TLS setting pair.
///
/// Adds a default URL scheme, validates TLS configuration, and returns the
/// effective TLS mode. Diagnostics are emitted for invalid combinations.
auto normalize_url_and_tls(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           const caf::actor_system_config* cfg = nullptr,
                           tls_options::options options = {.is_server = false})
  -> failure_or<bool>;

/// Builds an `HttpPoolConfig` from a URL and optional TLS configuration.
///
/// This mutates `url` the same way as `normalize_url_and_tls` and attaches the
/// resolved SSL context when TLS is enabled.
auto make_http_pool_config(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           std::chrono::milliseconds request_timeout,
                           const caf::actor_system_config* cfg = nullptr,
                           tls_options::options options = {.is_server = false})
  -> failure_or<HttpPoolConfig>;

/// Resolves user-provided HTTP header values that may reference secrets.
///
/// Literal string values are copied into `resolved_headers`. Secret values add
/// placeholder headers and return secret requests that fill those placeholders
/// before the request executes.
auto make_header_secret_requests(Option<located<data>> const& headers,
                                 std::vector<Header>& resolved_headers,
                                 diagnostic_handler& dh)
  -> std::vector<secret_request>;

/// Finds the first header by case-insensitive name.
auto find(std::span<Header const> headers, std::string_view name)
  -> Option<std::string>;

/// Removes all headers matching the case-insensitive name.
auto erase(std::vector<Header>& headers, std::string_view name) -> void;

/// Replaces all headers matching the case-insensitive name with one value.
auto set(std::vector<Header>& headers, std::string name, std::string value)
  -> void;

/// Base for parsed HTTP messages.
///
/// Used by operators that expose HTTP messages as data. The body is kept as a
/// string because these helpers model textual HTTP operator payloads.
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

/// A parsed HTTP request message.
struct Request : Message {
  std::string method;
  std::string uri;
};

/// One HTTPie-inspired request item.
///
/// Request items are the compact `key=value`, `Header: value`, and related
/// pieces that the curl-style connector accepts and later applies to an HTTP
/// request. Some parsed item kinds mirror HTTPie syntax that is not implemented
/// by `apply` yet and therefore produce an error if used.
struct RequestItem {
  /// Parses a request item like HTTPie.
  static auto parse(std::string_view str) -> Option<RequestItem>;

  /// The HTTP request part targeted by a parsed request item.
  enum ItemType : uint8_t {
    /// `key:=@path`: JSON request data read from a file.
    file_data_json,
    /// `key:=value`: JSON request data parsed from the item value.
    data_json,
    /// `key==value`: URL query parameter.
    url_param,
    /// `key=@path`: request data read from a file.
    file_data,
    /// `key@path`: form data read from a file.
    file_form,
    /// `key=value`: string request data.
    data,
    /// `Header:value`: request header.
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

/// Applies request items to an HTTP request.
///
/// This mutates the URL, headers, and body using HTTPie-compatible semantics.
auto apply(std::vector<RequestItem> items, Request& req) -> caf::error;

/// A request body after optional content encoding.
///
/// `content_encoding` is present only when `body` was transformed and should be
/// advertised with a `Content-Encoding` request header.
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
auto add_request_body_headers(std::vector<Header>& headers,
                              EncodedRequestBody const& body) -> void;

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

/// Supported pagination strategies for HTTP source operators.
enum class PaginationMode {
  /// Follow RFC 8288 Link headers with `rel=next`.
  link,
  /// Follow an OData collection envelope with `@odata.nextLink`.
  odata,
};

/// Parses a pagination strategy name.
auto parse_pagination_mode(std::string_view mode) -> Option<PaginationMode>;

/// Scans all Link response headers and returns the first resolved `rel=next`
/// URL, or None if no such link is present. Emits a warning on malformed
/// headers.
auto next_url_from_link_headers(std::vector<Header> const& response_headers,
                                std::string const& base_url,
                                location paginate_loc, diagnostic_handler& dh)
  -> Option<std::string>;

/// A decoded OData collection page.
///
/// `events` contains one output row per object in the envelope's `value` array.
/// `next_url` carries the opaque `@odata.nextLink` value when present.
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

/// Converts proxygen HTTP response into a `Response`.
auto to_http_response(proxygen::HTTPMessage const& headers) -> Response;

} // namespace tenzir::http

// Compatibility for out-of-tree plugins that included `tenzir/http.hpp` before
// using `HttpPool` or `HttpPoolConfig`.
#include <tenzir/http_pool.hpp>
