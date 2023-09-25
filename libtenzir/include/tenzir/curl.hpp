//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/data.hpp"
#include "tenzir/generator.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <curl/curl.h>

#include <chrono>
#include <map>
#include <span>
#include <string>

namespace tenzir {

/// The contents of a HTTP request body.
struct http_request_body {
  std::vector<std::byte> data;
  std::string content_type;
};

/// HTTP options for curl.
struct http_options {
  http_options();

  std::string method;
  std::map<std::string, std::string> headers;

  http_request_body body;
};

auto inspect(auto& f, http_options& x) -> bool {
  return f.object(x)
    .pretty_name("http_options")
    .fields(f.field("method", x.method), f.field("headers", x.headers),
            f.field("body.data", x.body.data),
            f.field("body.content_type", x.body.content_type));
}

/// Global options for the curl wrapper.
struct curl_options {
  std::string default_protocol{};
  std::string url{};
  http_options http{};
  bool verbose{false};
};

auto inspect(auto& f, curl_options& x) -> bool {
  return f.object(x)
    .pretty_name("curl_options")
    .fields(f.field("default_protocol", x.default_protocol),
            f.field("url", x.url), f.field("http", x.http),
            f.field("verbose", x.verbose));
}

/// A wrapper around the libcurl C API.
class curl {
public:
  /// Constructs a handle.
  explicit curl();

  curl(curl&) = delete;
  auto operator=(curl&) -> curl& = delete;
  curl(curl&&) = default;
  auto operator=(curl&&) -> curl& = default;

  ~curl();

  /// Sets options for subsequent operations.
  /// @param opts The options to set.
  auto set(const curl_options& opts) -> caf::error;

  /// Performs a HTTP request and retrieves the result as generator of byte
  /// chunks.
  /// @param timeout The poll timeout that drives the internal loop to fetch
  /// chunks of data.
  auto download(std::chrono::milliseconds timeout)
    -> generator<caf::expected<chunk_ptr>>;

  /// Performs a HTTP request and uses the provided bytes as request body.
  /// @param bytes The bytes to put in the request body.
  auto upload(std::span<const std::byte> bytes) -> caf::error;

  /// URL-encodes a given string.
  /// @param str The input to encode.
  /// @returns The encoded string.
  auto escape(std::string_view str) -> std::string;

  /// URL-encodes a record of parameters.
  /// @param xs The key-value pairs to encode.
  /// @returns The encoded string.
  auto escape(const record& xs) -> std::string;

private:
  static auto to_error(CURLcode number) -> caf::error;

  static auto to_error(CURLMcode number) -> caf::error;

  static auto write_callback(void* ptr, size_t size, size_t nmemb,
                             void* user_data) -> size_t;

  static auto read_callback(char* buffer, size_t size, size_t nitems,
                            void* user_data) -> size_t;

  auto set(CURLoption option, auto parameter) -> caf::error;

  auto set_body_data(std::span<const std::byte> bytes) -> caf::error;

  CURL* easy_{nullptr};
  CURLM* multi_{nullptr};
  curl_slist* headers_{nullptr};
};

} // namespace tenzir
