//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/curl.hpp"

#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/error.hpp"

#include <fmt/format.h>

namespace tenzir::curl {

namespace {} // namespace

easy::easy() : easy_{curl_easy_init()} {
  TENZIR_ASSERT(easy_ != nullptr);
}

easy::~easy() {
  if (easy_ != nullptr)
    curl_easy_cleanup(easy_);
  if (headers_ != nullptr)
    curl_slist_free_all(headers_);
}

auto easy::set(CURLoption option, long parameter) -> code {
  auto curl_code = curl_easy_setopt(easy_, option, parameter);
  return static_cast<code>(curl_code);
}

auto easy::set(CURLoption option, std::string_view parameter) -> code {
  auto curl_code = curl_easy_setopt(easy_, option, parameter.data());
  return static_cast<code>(curl_code);
}

auto easy::set(write_callback fun) -> code {
  TENZIR_ASSERT_CHEAP(fun);
  on_write_ = std::make_unique<write_callback>(std::move(fun));
  auto curl_code = curl_easy_setopt(easy_, CURLOPT_WRITEFUNCTION, on_write);
  TENZIR_ASSERT_CHEAP(curl_code == CURLE_OK);
  curl_code = curl_easy_setopt(easy_, CURLOPT_WRITEDATA, on_write_.get());
  return static_cast<code>(curl_code);
}

auto easy::set(read_callback fun) -> code {
  TENZIR_ASSERT_CHEAP(fun);
  on_read_ = std::make_unique<read_callback>(std::move(fun));
  auto curl_code = curl_easy_setopt(easy_, CURLOPT_READFUNCTION, on_read);
  TENZIR_ASSERT_CHEAP(curl_code == CURLE_OK);
  curl_code = curl_easy_setopt(easy_, CURLOPT_READDATA, on_read_.get());
  return static_cast<code>(curl_code);
}

auto easy::set_http_header(std::string_view name, std::string_view value)
  -> code {
  auto header = fmt::format("{}: {}", name, value);
  headers_ = curl_slist_append(headers_, header.c_str());
  auto curl_code = curl_easy_setopt(easy_, CURLOPT_HTTPHEADER, headers_);
  return static_cast<code>(curl_code);
}

auto easy::headers()
  -> generator<std::pair<std::string_view, std::string_view>> {
  const auto* current = headers_;
  while (current != nullptr) {
    auto str = std::string_view{current->data};
    auto split = detail::split(str, ": ");
    TENZIR_ASSERT_CHEAP(split.size() == 2);
    co_yield std::pair{split[0], split[1]};
    current = current->next;
  }
}

auto easy::perform() -> code {
  auto curl_code = curl_easy_perform(easy_);
  return static_cast<code>(curl_code);
}

auto easy::reset() -> void {
  curl_easy_reset(easy_);
}

auto easy::on_write(void* ptr, size_t size, size_t nmemb, void* user_data)
  -> size_t {
  TENZIR_ASSERT(size == 1);
  TENZIR_ASSERT(user_data != nullptr);
  const auto* data = reinterpret_cast<const std::byte*>(ptr);
  auto bytes = std::span<const std::byte>{data, nmemb};
  auto* f = reinterpret_cast<write_callback*>(user_data);
  (*f)(bytes);
  return nmemb;
}

auto easy::on_read(char* buffer, size_t size, size_t nitems, void* user_data)
  -> size_t {
  TENZIR_ASSERT(size == 1);
  TENZIR_ASSERT(user_data != nullptr);
  auto* ptr = reinterpret_cast<std::byte*>(buffer);
  auto output = std::span<std::byte>{ptr, size * nitems};
  auto* f = reinterpret_cast<read_callback*>(user_data);
  auto n = (*f)(output);
  TENZIR_ASSERT_CHEAP(n > 0);
  return n;
}

auto to_string(easy::code code) -> std::string_view {
  auto curl_code = static_cast<CURLcode>(code);
  return {curl_easy_strerror(curl_code)};
}

auto to_error(easy::code code) -> caf::error {
  if (code == easy::code::ok)
    return {};
  return caf::make_error(ec::unspecified,
                         fmt::format("curl: {}", to_string(code)));
}

multi::multi() : multi_{curl_multi_init()} {
}

multi::~multi() {
  // libcurl demands the following cleanup order:
  // (1) Remove easy handles
  // (2) Cleanup easy handles
  // (3) Clean up the multi handle
  // We cannot enforce (1) and (2) here because our easy handles don't have
  // shared ownership semantics. It's up to the user to add and remove them.
  curl_multi_cleanup(multi_);
}

auto multi::add(easy& handle) -> code {
  auto curl_code = curl_multi_add_handle(multi_, handle.easy_);
  return static_cast<code>(curl_code);
}

auto multi::remove(easy& handle) -> code {
  auto curl_code = curl_multi_remove_handle(multi_, handle.easy_);
  return static_cast<code>(curl_code);
}

auto multi::poll(std::chrono::milliseconds timeout) -> code {
  auto ms = detail::narrow_cast<int>(timeout.count());
  auto curl_code = curl_multi_poll(multi_, nullptr, 0u, ms, nullptr);
  return static_cast<code>(curl_code);
}

auto multi::perform() -> std::pair<code, size_t> {
  auto num_running = int{0};
  auto curl_code = curl_multi_perform(multi_, &num_running);
  TENZIR_ASSERT_CHEAP(num_running >= 0);
  return {static_cast<code>(curl_code),
          detail::narrow_cast<size_t>(num_running)};
}

auto multi::run(std::chrono::milliseconds timeout) -> caf::expected<size_t> {
  auto [result, num_running] = perform();
  if (result == code::ok and num_running != 0)
    result = poll(timeout);
  if (result != code::ok)
    return to_error(result);
  return num_running;
}

auto multi::loop(std::chrono::milliseconds timeout) -> caf::error {
  while (true) {
    if (auto num_running = run(timeout)) {
      if (*num_running == 0)
        return {};
    } else {
      return num_running.error();
    }
  }
}

auto to_string(multi::code code) -> std::string_view {
  auto curl_code = static_cast<CURLMcode>(code);
  return {curl_multi_strerror(curl_code)};
}

auto escape(std::string_view str) -> std::string {
  auto* easy = curl_easy_init();
  auto result = std::string{};
  auto length = detail::narrow_cast<int>(str.size());
  if (auto* escaped = curl_easy_escape(easy, str.data(), length)) {
    result = escaped;
    curl_free(escaped);
  }
  curl_easy_cleanup(easy);
  return result;
}

auto escape(const record& xs) -> std::string {
  auto to_raw_string = [](const auto& value) -> std::string {
    auto f = detail::overload{
      [&](const auto& x) {
        using tenzir::to_string;
        return to_string(x);
      },
      [](const std::basic_string<std::byte>& blob) {
        const auto* ptr = reinterpret_cast<const char*>(blob.data());
        return std::string{ptr, blob.size()};
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

auto to_error(multi::code code) -> caf::error {
  if (code == multi::code::ok)
    return {};
  return caf::make_error(ec::unspecified,
                         fmt::format("curl: {}", to_string(code)));
}

} // namespace tenzir::curl
