//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/result.hpp"
#include "tenzir/blob.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/option.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/tls_options.hpp"

#include <arrow/util/compression.h>
#include <caf/error.hpp>
#include <folly/io/async/SSLContext.h>
#include <wangle/ssl/SSLContextConfig.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::http {

auto add_default_url_scheme(std::string& url, bool tls_enabled) -> void;

auto is_tls_enabled(Option<located<data>> const& tls,
                    tls_options::options options,
                    std::string_view url_when_missing) -> bool;

auto normalize_url_and_tls(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           tls_options::options options = {.is_server = false})
  -> failure_or<bool>;

auto make_http_pool_config(Option<located<data>> const& tls, std::string& url,
                           location url_loc, diagnostic_handler& dh,
                           std::chrono::milliseconds request_timeout,
                           tls_options::options options = {.is_server = false})
  -> failure_or<HttpPoolConfig>;

auto make_header_secret_requests(
  Option<located<data>> const& headers,
  std::vector<std::pair<std::string, std::string>>& resolved_headers,
  diagnostic_handler& dh) -> std::vector<secret_request>;

auto parse_folly_tls_version(std::string_view input)
  -> Option<folly::SSLContext::SSLVersion>;

auto make_folly_tls_config(Option<located<data>> const& tls, location primary,
                           diagnostic_handler& dh, tls_options::options options)
  -> failure_or<wangle::SSLContextConfig>;

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

} // namespace tenzir::http
