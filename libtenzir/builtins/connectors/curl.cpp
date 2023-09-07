//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <curl/curl.h>

#include <filesystem>
#include <system_error>

using namespace std::chrono_literals;

namespace tenzir::plugins {

namespace {

/// Options for the curl wrapper.
struct curl_options {
  std::string default_protocol;
  std::string url;
};

/// A wrapper around the C API.
class curl {
  static auto to_error(CURLcode number) -> caf::error {
    if (number == CURLE_OK)
      return {};
    const auto* str = curl_easy_strerror(number);
    return caf::make_error(ec::unspecified, fmt::format("curl: {}", str));
  }

  static auto to_error(CURLMcode number) -> caf::error {
    if (number == CURLM_OK)
      return {};
    const auto* str = curl_multi_strerror(number);
    return caf::make_error(ec::unspecified, fmt::format("curl: {}", str));
  }

  static auto write_callback(void* ptr, size_t size, size_t nmemb,
                             void* user_data) -> size_t {
    TENZIR_ASSERT(size == 1);
    TENZIR_ASSERT(user_data != nullptr);
    const auto* data = reinterpret_cast<const std::byte*>(ptr);
    auto bytes = std::span<const std::byte>{data, nmemb};
    auto* chunks = reinterpret_cast<std::vector<chunk_ptr>*>(user_data);
    chunks->emplace_back(chunk::copy(bytes));
    return nmemb;
  }

public:
  explicit curl() : easy_{curl_easy_init()}, multi_{curl_multi_init()} {
    auto err = set(CURLOPT_FOLLOWLOCATION, 1L);
    TENZIR_ASSERT(not err);
    err = set(CURLOPT_WRITEFUNCTION, write_callback);
    TENZIR_ASSERT(not err);
    err = to_error(curl_multi_add_handle(multi_, easy_));
    TENZIR_ASSERT(not err);
  }

  curl(curl&) = delete;
  auto operator=(curl&) -> curl& = delete;
  curl(curl&&) = default;
  auto operator=(curl&&) -> curl& = default;

  ~curl() {
    curl_multi_remove_handle(multi_, easy_);
    curl_easy_cleanup(easy_);
    curl_multi_cleanup(multi_);
  }

  auto set(const curl_options& opts) -> caf::error {
    if (not opts.default_protocol.empty())
      if (auto err
          = set(CURLOPT_DEFAULT_PROTOCOL, opts.default_protocol.c_str()))
        return err;
    if (not opts.url.empty())
      if (auto err = set(CURLOPT_URL, opts.url.c_str()))
        return err;
    return {};
  }

  auto set(CURLoption option, auto parameter) -> caf::error {
    return to_error(curl_easy_setopt(easy_, option, parameter));
  }

  auto download(std::chrono::milliseconds timeout = 1000ms)
    -> generator<chunk_ptr> {
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
      for (auto&& chunk : chunks)
        co_yield chunk;
      chunks.clear();
    } while (still_running != 0);
  }

private:
  CURL* easy_;
  CURLM* multi_;
};

struct connector_args {
  located<std::string> url;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("connector_args")
      .fields(f.field("url", x.url));
  }
};

template <detail::string_literal Protocol>
class curl_loader final : public plugin_loader {
public:
  curl_loader() = default;

  curl_loader(connector_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto opts = curl_options{
      .default_protocol = name(),
      .url = args_.url.inner,
    };
    auto make = [&ctrl](curl_options opts) mutable -> generator<chunk_ptr> {
      auto handle = curl{};
      if (auto err = handle.set(opts))
        ctrl.abort(err);
      for (auto&& chunk : handle.download())
        co_yield chunk;
    };
    return make(std::move(opts));
  }

  auto name() const -> std::string override {
    return std::string{Protocol.str()};
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, curl_loader& x) -> bool {
    return f.object(x)
      .pretty_name("curl_loader")
      .fields(f.field("args", x.args_));
  }

  auto to_string() const -> std::string override {
    auto result = name();
    result += fmt::format(" {}", args_.url);
    return result;
  }

private:
  connector_args args_;
};

template <detail::string_literal Protocol>
class plugin final : public virtual loader_plugin<curl_loader<Protocol>> {
public:
  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = connector_args{};
    parser.add(args.url, "<url>");
    parser.parse(p);
    return std::make_unique<curl_loader<Protocol>>(std::move(args));
  }

  auto name() const -> std::string override {
    return std::string{Protocol.str()};
  }
};

} // namespace

// Available protocol names according to the documentation at
// https://curl.se/libcurl/c/CURLOPT_DEFAULT_PROTOCOL.html are: dict, file, ftp,
// ftps, gopher, http, https, imap, imaps, ldap, ldaps, pop3, pop3s, rtsp, scp,
// sftp, smb, smbs, smtp, smtps, telnet, tftp

using ftp = plugin<"ftp">;
using ftps = plugin<"ftps">;
using http = plugin<"http">;
using https = plugin<"https">;

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ftp)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ftps)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::https)
