//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/controller/api_support.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/tunnel/platform.hpp>

#include <fmt/format.h>
#include <folly/coro/DetachOnCancel.h>

#include <span>
#include <string>
#include <utility>

namespace tenzir {

namespace {

constexpr auto max_configuration_size = std::size_t{512} << 10;

auto fetch_configuration(std::string const& url, DiscoveryTimeouts timeouts)
  -> Result<std::string, std::string> {
  auto handle = curl::easy{};
  auto body = std::string{};
  handle.set(CURLOPT_URL, url);
  // Returning less than was handed over makes curl abort the transfer.
  handle.set_write_result_callback([&](std::span<std::byte const> data) {
    if (body.size() + data.size() > max_configuration_size) {
      return std::size_t{0};
    }
    body.append(reinterpret_cast<char const*>(data.data()), data.size());
    return data.size();
  });
  handle.set(CURLOPT_TIMEOUT_MS, static_cast<long>(timeouts.total.count()));
  handle.set(CURLOPT_CONNECTTIMEOUT_MS,
             static_cast<long>(timeouts.connect.count()));
  handle.set(CURLOPT_NOSIGNAL, 1L);
  handle.set(CURLOPT_FOLLOWLOCATION, 0L);
  handle.set(CURLOPT_NOPROXY, "localhost,127.0.0.0/8,::1");
  auto rc = handle.perform();
  if (rc == curl::easy::code::write_error) {
    return Err{fmt::format("discovery response exceeds {} KiB",
                           max_configuration_size >> 10)};
  }
  if (rc != curl::easy::code::ok) {
    return Err{fmt::format("discovery failed: {}", curl::to_string(rc))};
  }
  auto status = handle.get<curl::easy::info::response_code>().second;
  if (status != 200) {
    return Err{fmt::format("discovery returned HTTP {}", status)};
  }
  return body;
}

} // namespace

auto discovery_url(std::string_view platform_url)
  -> Result<std::string, std::string> {
  auto scheme_end = platform_url.find("://");
  if (scheme_end == std::string_view::npos
      or (platform_url.substr(0, scheme_end) != "http"
          and platform_url.substr(0, scheme_end) != "https")) {
    return Err{std::string{"platform URL must use http or https"}};
  }
  auto authority_end = platform_url.find_first_of("/?#", scheme_end + 3);
  auto authority
    = platform_url.substr(0, authority_end == std::string_view::npos
                               ? platform_url.size()
                               : authority_end);
  if (authority.size() == scheme_end + 3) {
    return Err{std::string{"platform URL has no host"}};
  }
  return fmt::format("{}/.well-known/tenzir-platform", authority);
}

namespace {

/// The HTTP URL under `name` in the platform's configuration.
auto http_url_field(std::string_view body, std::string_view name)
  -> Result<std::string, std::string> {
  auto parsed = api::Document::parse(body, "platform configuration");
  if (parsed.is_err()) {
    return Err{std::string{"discovery response is not valid JSON"}};
  }
  auto object
    = api::object_of(parsed.unwrap().root(), "platform configuration");
  if (object.is_err()) {
    return Err{std::string{"discovery response is not an object"}};
  }
  auto field = api::field(object.unwrap(), name, "platform configuration");
  if (field.is_err()) {
    return Err{fmt::format("discovery response has no {}", name)};
  }
  auto value = api::string_of(field.unwrap(), name);
  if (value.is_err()) {
    return Err{fmt::format("discovery {} is not a string", name)};
  }
  auto url = std::move(value).unwrap();
  if ((not url.starts_with("https://") and not url.starts_with("http://"))
      or url.find_first_of("?#") != std::string::npos) {
    return Err{fmt::format("discovery {} is not an HTTP URL", name)};
  }
  auto host_start = url.find("://") + 3;
  if (url.find_first_of('/', host_start) == host_start
      or host_start == url.size()) {
    return Err{fmt::format("discovery {} has no host", name)};
  }
  return url;
}

} // namespace

auto gateway_url_from_configuration(std::string_view body)
  -> Result<std::string, std::string> {
  return http_url_field(body, "gatewayUrl");
}

auto api_url_from_configuration(std::string_view body)
  -> Result<std::string, std::string> {
  return http_url_field(body, "apiUrl");
}

auto fetch_platform_configuration(std::string const& platform_url,
                                  DiscoveryTimeouts timeouts)
  -> Task<Result<std::string, std::string>> {
  auto url = discovery_url(platform_url);
  if (url.is_err()) {
    co_return Err{std::move(url).unwrap_err()};
  }
  co_return co_await folly::coro::detachOnCancel(
    spawn_blocking([address = std::move(url).unwrap(), timeouts] {
      return fetch_configuration(address, timeouts);
    }));
}

auto discover_gateway_url(std::string const& platform_url,
                          DiscoveryTimeouts timeouts)
  -> Task<Result<std::string, std::string>> {
  auto response = co_await fetch_platform_configuration(platform_url, timeouts);
  if (response.is_err()) {
    co_return Err{std::move(response).unwrap_err()};
  }
  co_return gateway_url_from_configuration(std::move(response).unwrap());
}

auto discover_api_url(std::string const& platform_url,
                      DiscoveryTimeouts timeouts)
  -> Task<Result<std::string, std::string>> {
  auto response = co_await fetch_platform_configuration(platform_url, timeouts);
  if (response.is_err()) {
    co_return Err{std::move(response).unwrap_err()};
  }
  co_return api_url_from_configuration(std::move(response).unwrap());
}

} // namespace tenzir
