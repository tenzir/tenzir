//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/curl.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/concept/parseable/numeric.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/error.hpp"
#include "tenzir/si_literals.hpp"

#include <fmt/format.h>

namespace tenzir::curl {

using namespace binary_byte_literals;

auto slist::append(std::string_view str) -> void {
  auto* slist = slist_.release();
  slist_.reset(curl_slist_append(slist, str.data()));
}

auto slist::items() const -> generator<std::string_view> {
  for (const auto* ptr = slist_.get(); ptr != nullptr; ptr = ptr->next) {
    co_yield std::string_view{ptr->data};
  }
}

auto on_write(void* ptr, size_t size, size_t nmemb, void* user_data) -> size_t {
  TENZIR_ASSERT(size == 1);
  TENZIR_ASSERT(user_data != nullptr);
  const auto* data = reinterpret_cast<const std::byte*>(ptr);
  auto bytes = std::span<const std::byte>{data, nmemb};
  auto* f = reinterpret_cast<write_callback*>(user_data);
  (*f)(bytes);
  return nmemb;
}

auto on_read(char* buffer, size_t size, size_t nitems, void* user_data)
  -> size_t {
  TENZIR_ASSERT(size == 1);
  TENZIR_ASSERT(user_data != nullptr);
  auto* ptr = reinterpret_cast<std::byte*>(buffer);
  auto output = std::span<std::byte>{ptr, size * nitems};
  auto* f = reinterpret_cast<read_callback*>(user_data);
  auto n = (*f)(output);
  return n;
}

easy::easy() : easy_{curl_easy_init()} {
  TENZIR_ASSERT(easy_ != nullptr);
}

auto easy::unset(CURLoption option) -> code {
  auto curl_code = curl_easy_setopt(easy_.get(), option, nullptr);
  return static_cast<code>(curl_code);
}

auto easy::set(CURLoption option, long parameter) -> code {
  auto curl_code = curl_easy_setopt(easy_.get(), option, parameter);
  return static_cast<code>(curl_code);
}

auto easy::set(CURLoption option, std::string_view parameter) -> code {
  auto curl_code = curl_easy_setopt(easy_.get(), option, parameter.data());
  return static_cast<code>(curl_code);
}

auto easy::set(write_callback fun) -> code {
  TENZIR_ASSERT(fun);
  on_write_ = std::make_unique<write_callback>(std::move(fun));
  auto curl_code
    = curl_easy_setopt(easy_.get(), CURLOPT_WRITEFUNCTION, on_write);
  TENZIR_ASSERT(curl_code == CURLE_OK);
  curl_code = curl_easy_setopt(easy_.get(), CURLOPT_WRITEDATA, on_write_.get());
  return static_cast<code>(curl_code);
}

auto easy::set(read_callback fun) -> code {
  TENZIR_ASSERT(fun);
  on_read_ = std::make_unique<read_callback>(std::move(fun));
  auto curl_code = curl_easy_setopt(easy_.get(), CURLOPT_READFUNCTION, on_read);
  TENZIR_ASSERT(curl_code == CURLE_OK);
  curl_code = curl_easy_setopt(easy_.get(), CURLOPT_READDATA, on_read_.get());
  return static_cast<code>(curl_code);
}

auto easy::set(mime handle) -> code {
  // We do not support reading MIME parts through a callback.
  auto curl_code = curl_easy_setopt(easy_.get(), CURLOPT_READFUNCTION, nullptr);
  TENZIR_ASSERT(curl_code == CURLE_OK);
  // Set MIME structure as the new thing.
  curl_code
    = curl_easy_setopt(easy_.get(), CURLOPT_MIMEPOST, handle.mime_.get());
  if (curl_code == CURLE_OK) {
    mime_ = std::make_unique<mime>(std::move(handle));
  }
  return static_cast<code>(curl_code);
}

auto easy::set_infilesize(long size) -> code {
  TENZIR_ASSERT(size >= 0);
  auto unsigned_size = detail::narrow_cast<uint64_t>(size);
  auto option
    = unsigned_size > 2_GiB ? CURLOPT_INFILESIZE_LARGE : CURLOPT_INFILESIZE;
  auto curl_code = curl_easy_setopt(easy_.get(), option, size);
  return static_cast<code>(curl_code);
}

auto easy::set_postfieldsize(long size) -> code {
  TENZIR_ASSERT(size >= 0);
  auto unsigned_size = detail::narrow_cast<uint64_t>(size);
  auto option = unsigned_size > 2_GiB ? CURLOPT_POSTFIELDSIZE_LARGE
                                      : CURLOPT_POSTFIELDSIZE;
  auto curl_code = curl_easy_setopt(easy_.get(), option, size);
  return static_cast<code>(curl_code);
}

auto easy::add_mail_recipient(std::string_view mail) -> easy::code {
  mail_recipients_.append(mail);
  auto curl_code = curl_easy_setopt(easy_.get(), CURLOPT_MAIL_RCPT,
                                    mail_recipients_.slist_.get());
  return static_cast<code>(curl_code);
}

auto easy::set_http_header(std::string_view name, std::string_view value)
  -> code {
  auto header_name = [](std::string_view str) {
    auto i = str.find(':');
    TENZIR_ASSERT(i != std::string_view::npos);
    return str.substr(0, i);
  };
  // Check if we are overwriting a header. Since slits are immutable, this
  // would require rebuilding the list (and has quadratic overhead).
  for (auto header : http_headers_.items()) {
    if (header_name(header) == name) {
      slist copy;
      for (auto item : http_headers_.items()) {
        if (header_name(item) != name) {
          copy.append(item);
        }
      }
      http_headers_ = std::move(copy);
      break;
    }
  }
  auto header = fmt::format("{}: {}", name, value);
  http_headers_.append(header);
  auto curl_code = curl_easy_setopt(easy_.get(), CURLOPT_HTTPHEADER,
                                    http_headers_.slist_.get());
  return static_cast<code>(curl_code);
}

auto easy::headers()
  -> generator<std::pair<std::string_view, std::string_view>> {
  for (auto str : http_headers_.items()) {
    auto split = detail::split(str, ": ");
    TENZIR_ASSERT(split.size() == 2);
    co_yield std::pair{split[0], split[1]};
  }
}

auto easy::perform() -> code {
  auto curl_code = curl_easy_perform(easy_.get());
  return static_cast<code>(curl_code);
}

auto easy::reset() -> void {
  curl_easy_reset(easy_.get());
}

auto to_string(easy::code code) -> std::string_view {
  auto curl_code = static_cast<CURLcode>(code);
  return {curl_easy_strerror(curl_code)};
}

auto to_error(easy::code code) -> caf::error {
  if (code == easy::code::ok) {
    return {};
  }
  return caf::make_error(ec::unspecified,
                         fmt::format("curl: {}", to_string(code)));
}

multi::multi() : multi_{curl_multi_init(), curlm_deleter{}} {
}

auto multi::add(easy& handle) -> code {
  auto curl_code = curl_multi_add_handle(multi_.get(), handle.easy_.get());
  return static_cast<code>(curl_code);
}

auto multi::remove(easy& handle) -> code {
  auto curl_code = curl_multi_remove_handle(multi_.get(), handle.easy_.get());
  return static_cast<code>(curl_code);
}

auto multi::poll(std::chrono::milliseconds timeout) -> code {
  auto ms = detail::narrow_cast<int>(timeout.count());
  auto curl_code = curl_multi_poll(multi_.get(), nullptr, 0u, ms, nullptr);
  return static_cast<code>(curl_code);
}

auto multi::perform() -> std::pair<code, size_t> {
  auto num_running = int{0};
  auto curl_code = curl_multi_perform(multi_.get(), &num_running);
  TENZIR_ASSERT(num_running >= 0);
  return {static_cast<code>(curl_code),
          detail::narrow_cast<size_t>(num_running)};
}

auto multi::run(std::chrono::milliseconds timeout) -> caf::expected<size_t> {
  auto [result, num_running] = perform();
  if (result == code::ok and num_running != 0) {
    result = poll(timeout);
  }
  if (result != code::ok) {
    return to_error(result);
  }
  return num_running;
}

auto multi::loop(std::chrono::milliseconds timeout) -> caf::error {
  while (true) {
    if (auto num_running = run(timeout)) {
      if (*num_running == 0) {
        return {};
      }
    } else {
      return num_running.error();
    }
  }
}

auto multi::info_read() -> generator<easy::code> {
  auto num_left = 0;
  CURLMsg* msg = nullptr;
  while ((msg = curl_multi_info_read(multi_.get(), &num_left))) {
    if (msg->msg == CURLMSG_DONE) {
      co_yield static_cast<easy::code>(msg->data.result);
    }
  }
}

auto to_string(multi::code code) -> std::string_view {
  auto curl_code = static_cast<CURLMcode>(code);
  return {curl_multi_strerror(curl_code)};
}

auto mime::part::name(std::string_view name) -> easy::code {
  TENZIR_ASSERT(part_ != nullptr);
  TENZIR_ASSERT(not name.empty());
  auto curl_code = curl_mime_name(part_, name.data());
  return static_cast<easy::code>(curl_code);
}

auto mime::part::type(std::string_view content_type) -> easy::code {
  TENZIR_ASSERT(part_ != nullptr);
  TENZIR_ASSERT(not content_type.empty());
  auto curl_code = curl_mime_type(part_, content_type.data());
  return static_cast<easy::code>(curl_code);
}

auto mime::part::data(std::span<const std::byte> buffer) -> easy::code {
  TENZIR_ASSERT(part_ != nullptr);
  TENZIR_ASSERT(not buffer.empty());
  const auto* ptr = reinterpret_cast<const char*>(buffer.data());
  auto curl_code = curl_mime_data(part_, ptr, buffer.size());
  return static_cast<easy::code>(curl_code);
}

auto mime::part::data(read_callback* on_read) -> easy::code {
  TENZIR_ASSERT(part_ != nullptr);
  TENZIR_ASSERT(on_read != nullptr);
  auto curl_code
    = curl_mime_data_cb(part_, -1, curl::on_read, nullptr, nullptr, on_read);
  return static_cast<easy::code>(curl_code);
}

mime::part::part(curl_mimepart* ptr) : part_{ptr} {
}

mime::mime(easy& handle)
  : mime_{curl_mime_init(handle.easy_.get()), curl_mime_deleter{}} {
}

auto mime::add() -> mime::part {
  return part{curl_mime_addpart(mime_.get())};
}

auto to_error(multi::code code) -> caf::error {
  if (code == multi::code::ok) {
    return {};
  }
  return caf::make_error(ec::unspecified,
                         fmt::format("curl: {}", to_string(code)));
}

url::url() : url_{curl_url()} {
}

url::url(const url& other) : url_{curl_url_dup(other.url_.get())} {
}

auto url::operator=(const url& other) -> url& {
  url_.reset(curl_url_dup(other.url_.get()));
  return *this;
}

auto url::set(part url_part, std::string_view str, flags fs) -> code {
  auto curl_part = static_cast<CURLUPart>(url_part);
  auto curl_flags = static_cast<std::underlying_type_t<flags>>(fs);
  auto curl_code = curl_url_set(url_.get(), curl_part, str.data(), curl_flags);
  return static_cast<code>(curl_code);
}

auto url::get(part url_part, unsigned int flags) const
  -> std::pair<code, std::optional<std::string>> {
  auto curl_part = static_cast<CURLUPart>(url_part);
  char* content = nullptr;
  auto curl_code = curl_url_get(url_.get(), curl_part, &content, flags);
  auto result = static_cast<code>(curl_code);
  if (result != code::ok or content == nullptr) {
    return {result, std::nullopt};
  }
  auto string = std::string{content};
  curl_free(content);
  return {result, std::move(string)};
}

/// @relates url
auto to_string(url::code code) -> std::string_view {
  auto curl_code = static_cast<CURLUcode>(code);
  return {curl_url_strerror(curl_code)};
}

auto to_string(const url& x) -> std::string {
  auto [code, result] = x.get(url::part::url);
  TENZIR_ASSERT(code);
  TENZIR_ASSERT(result);
  return std::move(*result);
}

/// @relates url
auto to_error(url::code code) -> caf::error {
  if (code == url::code::ok) {
    return {};
  }
  return caf::make_error(ec::unspecified,
                         fmt::format("curl: {}", to_string(code)));
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

auto set(easy& handle, chunk_ptr chunk) -> caf::error {
  TENZIR_ASSERT(chunk);
  TENZIR_ASSERT(chunk->size() > 0);
  auto size = detail::narrow_cast<long>(chunk->size());
  if (auto err = to_error(handle.set_infilesize(size))) {
    return err;
  }
  auto on_read = [chunk](std::span<std::byte> buffer) mutable -> size_t {
    if (not chunk or chunk->size() == 0) {
      return 0;
    }
    TENZIR_DEBUG("reading {} bytes into {}-byte buffer", chunk->size(),
                 buffer.size());
    if (buffer.size() >= chunk->size()) {
      // Read the chunk in one shot.
      std::memcpy(buffer.data(), chunk->data(), chunk->size());
      auto bytes_copied = chunk->size();
      chunk = {};
      return bytes_copied;
    }
    // Do multiple rounds.
    std::memcpy(buffer.data(), chunk->data(), buffer.size());
    chunk = chunk->slice(buffer.size());
    return buffer.size();
  };
  return to_error(handle.set(on_read));
}

} // namespace tenzir::curl
