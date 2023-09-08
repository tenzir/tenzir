//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/curl.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/config.hpp"
#include "tenzir/detail/assert.hpp"
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

auto curl::parse_url(const std::string& str) -> bool {
  auto* h = curl_url();
  auto result = curl_url_set(h, CURLUPART_URL, str.c_str(), 0) == CURLUE_OK;
  curl_url_cleanup(h);
  return result;
}

curl::curl() : easy_{curl_easy_init()}, multi_{curl_multi_init()} {
  auto err = set(CURLOPT_FOLLOWLOCATION, 1L);
  TENZIR_ASSERT(not err);
  err = set(CURLOPT_WRITEFUNCTION, write_callback);
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

auto curl::set(const curl_options& opts) -> caf::error {
  if (not opts.default_protocol.empty())
    if (auto err = set(CURLOPT_DEFAULT_PROTOCOL, opts.default_protocol.c_str()))
      return err;
  if (not opts.url.empty())
    if (auto err = set(CURLOPT_URL, opts.url.c_str()))
      return err;
  if (opts.default_protocol == "http" || opts.default_protocol == "https") {
    if (not opts.http.body.empty() || opts.http.method == "POST") {
      // Setting the size manually is important because curl would otherwise
      // use strlen to determine the body size. We may pass binary data at
      // some point.
      auto size = detail::narrow_cast<long>(opts.http.body.size());
      if (auto err = set(CURLOPT_POSTFIELDSIZE, size))
        return err;
      // Here is choice of CURLOPT_POSTFIELDS vs. CURLOPT_COPYPOSTFIELDS,
      // where the latter copies the input. We're copying now, but should
      // revisit this for large POST bodies.
      if (auto err = set(CURLOPT_COPYPOSTFIELDS, opts.http.body.c_str()))
        return err;
      // NB: setting body data via CURLOPT_POSTFIELDS implicitly sets the
      // Content-Type header to 'application/x-www-form-urlencoded' unless we
      // provide a Content-Type header that overrides it.
    }
    if (opts.http.method == "GET") {
      if (auto err = set(CURLOPT_HTTPGET, 1L))
        return err;
    } else if (opts.http.method == "POST") {
      if (auto err = set(CURLOPT_POST, 1L))
        return err;
    } else if (opts.http.method == "HEAD") {
      if (auto err = set(CURLOPT_NOBODY, 1L))
        return err;
    } else if (not opts.http.method.empty()) {
      if (auto err = set(CURLOPT_CUSTOMREQUEST, opts.http.method.c_str()))
        return err;
    }
    if (not opts.http.headers.empty()) {
      if (headers_) {
        curl_slist_free_all(headers_);
        headers_ = nullptr;
      }
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

auto curl::download(std::chrono::milliseconds timeout) -> generator<chunk_ptr> {
  // curl_easy_setopt(easy_, CURLOPT_VERBOSE, 1L);
  std::vector<chunk_ptr> chunks;
  auto err = set(CURLOPT_WRITEDATA, &chunks);
  TENZIR_ASSERT(not err);
  auto ms = detail::narrow_cast<int>(timeout.count());
  auto still_running = int{0};
  do {
    auto mc = curl_multi_perform(multi_, &still_running);
    if (mc == CURLM_OK && still_running != 0)
      mc = curl_multi_poll(multi_, nullptr, 0u, ms, nullptr);
    if (mc != CURLM_OK) {
      TENZIR_ERROR(to_error(mc));
      co_return;
    }
    if (chunks.empty())
      co_yield {};
    for (auto&& chunk : chunks)
      co_yield chunk;
    chunks.clear();
  } while (still_running != 0);
}

auto curl::set(CURLoption option, auto parameter) -> caf::error {
  return to_error(curl_easy_setopt(easy_, option, parameter));
}

} // namespace tenzir
