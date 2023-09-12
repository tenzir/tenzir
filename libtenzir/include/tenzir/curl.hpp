//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/generator.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <curl/curl.h>

#include <chrono>
#include <map>
#include <string>

namespace tenzir {

/// HTTP options for curl.
struct http_options {
  http_options();

  std::string method;
  std::map<std::string, std::string> headers;
  std::string body;
};

auto inspect(auto& f, http_options& x) -> bool {
  return f.object(x)
    .pretty_name("http_options")
    .fields(f.field("method", x.method), f.field("headers", x.headers),
            f.field("body", x.body));
}

/// Options for the curl wrapper.
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

/// A wrapper around the C API.
class curl {
  static auto to_error(CURLcode number) -> caf::error;

  static auto to_error(CURLMcode number) -> caf::error;

  static auto write_callback(void* ptr, size_t size, size_t nmemb,
                             void* user_data) -> size_t;

public:
  explicit curl();

  curl(curl&) = delete;
  auto operator=(curl&) -> curl& = delete;
  curl(curl&&) = default;
  auto operator=(curl&&) -> curl& = default;

  ~curl();

  auto set(const curl_options& opts) -> caf::error;

  auto download(std::chrono::milliseconds timeout)
    -> generator<caf::expected<chunk_ptr>>;

private:
  auto set(CURLoption option, auto parameter) -> caf::error;

  CURL* easy_{nullptr};
  CURLM* multi_{nullptr};
  curl_slist* headers_{nullptr};
};

} // namespace tenzir
