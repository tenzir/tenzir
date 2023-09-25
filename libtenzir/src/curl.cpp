//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/curl.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/config.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/error.hpp"

#include <fmt/format.h>

#include <span>

using namespace std::chrono_literals;

namespace tenzir {

/// HTTP options for curl.
http_options::http_options() {
  static const auto default_headers = std::map<std::string, std::string>{
    {"Accept", "*/*"},
    {"User-Agent", fmt::format("Tenzir/{}", version::version)},
  };
  headers = default_headers;
}

curl::curl() : easy_{curl_easy_init()}, multi_{curl_multi_init()} {
  auto err = set(CURLOPT_FOLLOWLOCATION, 1L);
  TENZIR_ASSERT(not err);
  // Enable all supported built-in compressions by setting the empty string.
  // This can always be overriden by manually setting the Accept-Encoding
  // header.
  err = set(CURLOPT_ACCEPT_ENCODING, "");
  TENZIR_ASSERT(not err);
  err = to_error(curl_multi_add_handle(multi_, easy_));
  TENZIR_ASSERT(not err);
}

curl::~curl() {
  if (headers_)
    curl_slist_free_all(headers_);
  curl_multi_remove_handle(multi_, easy_);
  curl_easy_cleanup(easy_);
  curl_multi_cleanup(multi_);
}

auto curl::download(std::chrono::milliseconds timeout)
  -> generator<caf::expected<chunk_ptr>> {
  auto err = set(CURLOPT_WRITEFUNCTION, write_callback);
  TENZIR_ASSERT(not err);
  std::vector<chunk_ptr> chunks;
  err = set(CURLOPT_WRITEDATA, &chunks);
  TENZIR_ASSERT(not err);
  auto guard = caf::detail::make_scope_guard([this]() {
    auto err = set(CURLOPT_WRITEFUNCTION, nullptr);
    TENZIR_ASSERT(not err);
    err = set(CURLOPT_WRITEDATA, nullptr);
    TENZIR_ASSERT(not err);
  });
  auto ms = detail::narrow_cast<int>(timeout.count());
  auto still_running = int{0};
  do {
    auto mc = curl_multi_perform(multi_, &still_running);
    if (mc == CURLM_OK && still_running != 0)
      mc = curl_multi_poll(multi_, nullptr, 0u, ms, nullptr);
    if (mc != CURLM_OK) {
      co_yield to_error(mc);
      co_return;
    }
    if (chunks.empty())
      co_yield chunk_ptr{};
    for (auto&& chunk : chunks)
      co_yield chunk;
    chunks.clear();
  } while (still_running != 0);
}

auto curl::upload(std::span<const std::byte> bytes) -> caf::error {
  if (auto err = set_body_data(bytes))
    return err;
  auto still_running = int{0};
  do {
    auto mc = curl_multi_perform(multi_, &still_running);
    if (mc == CURLM_OK && still_running != 0)
      mc = curl_multi_poll(multi_, nullptr, 0u, 0, nullptr);
    if (mc != CURLM_OK)
      return to_error(mc);
  } while (still_running != 0);
  return {};
}

auto curl::escape(std::string_view str) -> std::string {
  auto result = std::string{};
  auto length = detail::narrow_cast<int>(str.size());
  if (auto* escaped = curl_easy_escape(easy_, str.data(), length)) {
    result = escaped;
    curl_free(escaped);
  }
  return result;
}

auto curl::escape(const record& xs) -> std::string {
  auto to_raw_string = [](const auto& value) -> std::string {
    auto f = detail::overload{
      [&](const auto& x) {
        return to_string(x);
      },
      [](const std::string& str) {
        return str; // no more double quotes
      },
    };
    return caf::visit(f, value);
  };
  std::vector<std::string> kvps;
  kvps.reserve(xs.size());
  for (const auto& [key, value] : xs) {
    auto escaped_key = escape(key);
    auto escaped_value = escape(to_raw_string(value));
    kvps.push_back(fmt::format("{}={}", escaped_key, escaped_value));
  }
  return fmt::format("{}", fmt::join(kvps, "&"));
}

auto curl::set(CURLoption option, auto parameter) -> caf::error {
  return to_error(curl_easy_setopt(easy_, option, parameter));
}

/// A wrapper around the C API.
auto curl::to_error(CURLcode number) -> caf::error {
  if (number == CURLE_OK)
    return {};
  const auto* str = curl_easy_strerror(number);
  return caf::make_error(ec::unspecified, fmt::format("curl: {}", str));
}

auto curl::to_error(CURLMcode number) -> caf::error {
  if (number == CURLM_OK)
    return {};
  const auto* str = curl_multi_strerror(number);
  return caf::make_error(ec::unspecified, fmt::format("curl: {}", str));
}

auto curl::write_callback(void* ptr, size_t size, size_t nmemb, void* user_data)
  -> size_t {
  TENZIR_ASSERT(size == 1);
  TENZIR_ASSERT(user_data != nullptr);
  const auto* data = reinterpret_cast<const std::byte*>(ptr);
  auto bytes = std::span<const std::byte>{data, nmemb};
  auto* chunks = reinterpret_cast<std::vector<chunk_ptr>*>(user_data);
  chunks->emplace_back(chunk::copy(bytes));
  return nmemb;
}

auto curl::read_callback(char* buffer, size_t size, size_t nitems,
                         void* user_data) -> size_t {
  // Returning 0 signals end-of-file to the library and causes it to stop the
  // current transfer.
  TENZIR_ASSERT(size == 1);
  TENZIR_ASSERT(user_data != nullptr);
  auto* ptr = reinterpret_cast<std::byte*>(buffer);
  auto output = std::span<std::byte>{ptr, size * nitems};
  auto* input = reinterpret_cast<std::span<const std::byte>*>(user_data);
  auto n = std::min(output.size(), input->size());
  TENZIR_ASSERT(n > 0);
  std::copy_n(input->begin(), n, output.end());
  return n;
}

auto curl::set(const curl_options& opts) -> caf::error {
  if (opts.verbose)
    if (auto err = set(CURLOPT_VERBOSE, 1L))
      return err;
  if (not opts.default_protocol.empty())
    if (auto err = set(CURLOPT_DEFAULT_PROTOCOL, opts.default_protocol.c_str()))
      return err;
  if (not opts.url.empty())
    if (auto err = set(CURLOPT_URL, opts.url.c_str()))
      return err;
  if (opts.default_protocol == "http" or opts.default_protocol == "https") {
    // For POST and PUT, we also set an empty body.
    if (not opts.http.body.data.empty() or opts.http.method == "POST"
        or opts.http.method == "PUT") {
      if (auto err = set_body_data(as_bytes(opts.http.body.data)))
        return err;
    }
    // Configure curl based on HTTP method.
    if (opts.http.method == "GET") {
      if (auto err = set(CURLOPT_HTTPGET, 1L))
        return err;
    } else if (opts.http.method == "HEAD") {
      if (auto err = set(CURLOPT_NOBODY, 1L))
        return err;
    } else if (opts.http.method == "POST") {
      if (auto err = set(CURLOPT_POST, 1L))
        return err;
    } else if (not opts.http.method.empty()) {
      if (auto err = set(CURLOPT_CUSTOMREQUEST, opts.http.method.c_str()))
        return err;
    }
    if (headers_) {
      curl_slist_free_all(headers_);
      headers_ = nullptr;
    }
    // Add Content-Type to headers if user specified it for the request body.
    if (not opts.http.body.content_type.empty()) {
      auto content_type
        = fmt::format("Content-Type: {}", opts.http.body.content_type);
      headers_ = curl_slist_append(headers_, content_type.c_str());
    }
    if (not opts.http.headers.empty()) {
      for (const auto& [header, value] : opts.http.headers) {
        auto str = header;
        str += ':';
        if (not value.empty()) {
          str += ' ';
          str += value;
        }
        headers_ = curl_slist_append(headers_, str.c_str());
      }
    }
    if (auto err = set(CURLOPT_HTTPHEADER, headers_))
      return err;
  }
  return {};
}

auto curl::set_body_data(std::span<const std::byte> bytes) -> caf::error {
  // Setting the size manually is important because curl would otherwise
  // use strlen to determine the body size (which would be UB for binary
  // data).
  auto size = detail::narrow_cast<long>(bytes.size());
  if (auto err = set(CURLOPT_POSTFIELDSIZE, size))
    return err;
  // Here is choice of CURLOPT_POSTFIELDS vs. CURLOPT_COPYPOSTFIELDS,
  // where the latter copies the input. We're copying now, but should
  // revisit this for large POST bodies.
  if (auto err = set(CURLOPT_COPYPOSTFIELDS, bytes.data()))
    return err;
  // NB: setting body data via CURLOPT_POSTFIELDS implicitly sets the
  // Content-Type header to 'application/x-www-form-urlencoded' unless we
  // provide a Content-Type header that overrides it.
  return {};
}

} // namespace tenzir
