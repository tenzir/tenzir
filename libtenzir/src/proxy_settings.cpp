//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/proxy_settings.hpp"

#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/subnet.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/error.hpp"
#include "tenzir/ip.hpp"

#include <arrow/status.h>
#include <arrow/util/uri.h>
#include <caf/settings.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <initializer_list>
#include <ranges>
#include <string>
#include <string_view>

namespace tenzir {

namespace {

// The single resolved instance. Written exactly once from main() under
// `initialize_proxy_settings`, then read-only for the rest of the process.
auto mutable_proxy_settings() -> proxy_settings& {
  static auto instance = proxy_settings{};
  return instance;
}

auto to_lower(std::string_view input) -> std::string {
  auto result = std::string{input};
  std::ranges::transform(result, result.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return result;
}

// Picks a proxy URL from env vars when config does not provide one.
auto proxy_from_env(std::initializer_list<char const*> names)
  -> Option<std::string> {
  for (auto const* name : names) {
    if (auto value = detail::getenv(name)) {
      auto trimmed = std::string{detail::trim(*value)};
      if (not trimmed.empty()) {
        return Option<std::string>{std::move(trimmed)};
      }
    }
  }
  return {};
}

auto no_proxy_from_env() -> Option<std::string> {
  if (auto value = detail::getenv("TENZIR_NO_PROXY")) {
    auto trimmed = std::string{detail::trim(*value)};
    if (trimmed.empty()) {
      return Option<std::string>{};
    }
    return Option<std::string>{std::move(trimmed)};
  }
  for (auto const* name : {"NO_PROXY", "no_proxy"}) {
    if (auto value = detail::getenv(name)) {
      auto trimmed = std::string{detail::trim(*value)};
      if (not trimmed.empty()) {
        return Option<std::string>{std::move(trimmed)};
      }
    }
  }
  return {};
}

auto no_proxy_from_config(caf::settings const& options)
  -> caf::expected<Option<std::string>> {
  auto const* value = caf::get_if(&options, "tenzir.no-proxy");
  if (not value) {
    return Option<std::string>{};
  }
  if (auto const* str = caf::get_if<std::string>(value)) {
    auto trimmed = std::string{detail::trim(*str)};
    return Option<std::string>{std::move(trimmed)};
  }
  auto entries = detail::unpack_config_list_to_vector<std::string>(*value);
  if (not entries) {
    return std::move(entries.error());
  }
  auto result = std::string{};
  for (auto const& entry : *entries) {
    auto trimmed = detail::trim(entry);
    if (trimmed.empty()) {
      continue;
    }
    if (not result.empty()) {
      result += ',';
    }
    result += trimmed;
  }
  return Option<std::string>{std::move(result)};
}

auto proxy_from_config(caf::settings const& options,
                       std::string_view config_key)
  -> caf::expected<Option<std::string>> {
  auto const* value = caf::get_if(&options, std::string{config_key});
  if (not value) {
    return Option<std::string>{};
  }
  auto const* str = caf::get_if<std::string>(value);
  if (not str) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} must be a string", config_key));
  }
  return Option<std::string>{*str};
}

auto append_no_proxy_entries(std::string& result, std::string_view entries)
  -> void {
  for (auto part : std::views::split(entries, ',')) {
    auto entry = detail::trim(std::string_view{part.begin(), part.end()});
    if (entry.empty()) {
      continue;
    }
    result += ',';
    result += entry;
  }
}

// Validates the proxy URL and splits it into its components. The URL must parse
// and must have an explicit port: a missing port would fall back to libcurl's
// port-1080 default (a SOCKS port), which is almost certainly not what the user
// intended.
auto parse_proxy_url(std::string_view config_key, std::string const& url)
  -> caf::expected<proxy_url> {
  auto parsed = arrow::util::Uri{};
  if (auto status = parsed.Parse(url); not status.ok()) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} is not a valid URL: {}", config_key,
                                       status.ToStringWithoutContextLines()));
  }
  auto scheme = parsed.scheme();
  if (scheme != "http" and scheme != "https") {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} has unsupported "
                                       "scheme `{}`; expected "
                                       "http or https",
                                       config_key, scheme));
  }
  auto host = parsed.host();
  if (host.empty()) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} is missing a host", config_key));
  }
  auto port_text = parsed.port_text();
  if (port_text.empty()) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} must include an explicit port "
                                       "(e.g. http://proxy.example.com:3128)",
                                       config_key));
  }
  auto port = 0u;
  try {
    port = static_cast<unsigned>(std::stoul(port_text));
  } catch (std::exception const& e) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} has invalid port "
                                       "`{}`: {}",
                                       config_key, port_text, e.what()));
  }
  if (port == 0 or port > 65535) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("{} port `{}` is out of range",
                                       config_key, port_text));
  }
  auto out = proxy_url{};
  out.url = url;
  out.scheme = std::move(scheme);
  out.host = std::move(host);
  out.port = static_cast<uint16_t>(port);
  if (auto user = parsed.username(); not user.empty()) {
    out.username = std::move(user);
  }
  if (auto pw = parsed.password(); not pw.empty()) {
    out.password = std::move(pw);
  }
  return out;
}

// Mirrors the resolved proxy back into the process environment. Called
// exactly once from main() while still single-threaded, so the classic
// glibc setenv/getenv race does not apply: no other thread exists yet.
// AWS SDK, gRPC, and libcurl-backed cloud SDKs (Arrow GCS, Arrow Azure,
// Snowflake ADBC, the platform WebSocket client) read these env vars on first
// use.
auto mirror_to_environment(proxy_settings const& ps) -> caf::error {
  auto set = [](char const* name, std::string const& value) -> caf::error {
    if (auto err = detail::setenv(name, value, /*overwrite=*/1)) {
      return err;
    }
    return {};
  };
  if (ps.http_proxy) {
    for (auto const* name : {"HTTP_PROXY", "http_proxy"}) {
      if (auto err = set(name, ps.http_proxy->url)) {
        return err;
      }
    }
  }
  if (ps.https_proxy) {
    for (auto const* name : {"HTTPS_PROXY", "https_proxy"}) {
      if (auto err = set(name, ps.https_proxy->url)) {
        return err;
      }
    }
  }
  if (ps.http_proxy or ps.https_proxy or ps.no_proxy) {
    auto no_proxy = effective_no_proxy(ps);
    for (auto const* name : {"NO_PROXY", "no_proxy"}) {
      if (auto err = set(name, no_proxy)) {
        return err;
      }
    }
  }
  return {};
}

} // namespace

auto initialize_proxy_settings(caf::settings const& options) -> caf::error {
  auto& ps = mutable_proxy_settings();
  ps = proxy_settings{};
  // Proxy URLs fall back to direct env reads when their config keys are absent
  // or empty. For `tenzir.no-proxy`, only an absent key falls back to env.
  auto resolve_proxy = [&](std::string_view config_key,
                           std::initializer_list<char const*> env_names)
    -> caf::expected<Option<proxy_url>> {
    auto configured_proxy = proxy_from_config(options, config_key);
    if (not configured_proxy) {
      return std::move(configured_proxy.error());
    }
    auto resolved = [&]() -> Option<std::string> {
      if (*configured_proxy and not(*configured_proxy)->empty()) {
        return Option<std::string>{**configured_proxy};
      }
      return proxy_from_env(env_names);
    }();
    if (not resolved) {
      return Option<proxy_url>{};
    }
    auto parsed = parse_proxy_url(config_key, *resolved);
    if (not parsed) {
      return std::move(parsed.error());
    }
    return Option<proxy_url>{std::move(*parsed)};
  };
  auto http_proxy = resolve_proxy(
    "tenzir.http-proxy", {"TENZIR_HTTP_PROXY", "HTTP_PROXY", "http_proxy"});
  if (not http_proxy) {
    return std::move(http_proxy.error());
  }
  auto https_proxy = resolve_proxy(
    "tenzir.https-proxy", {"TENZIR_HTTPS_PROXY", "HTTPS_PROXY", "https_proxy"});
  if (not https_proxy) {
    return std::move(https_proxy.error());
  }
  auto resolved_no_proxy = no_proxy_from_config(options);
  if (not resolved_no_proxy) {
    return std::move(resolved_no_proxy.error());
  }
  if (not *resolved_no_proxy) {
    *resolved_no_proxy = no_proxy_from_env();
  }
  ps.http_proxy = std::move(*http_proxy);
  ps.https_proxy = std::move(*https_proxy);
  if (*resolved_no_proxy) {
    ps.no_proxy = std::move(*resolved_no_proxy);
  }
  // Mirror to env so SDKs we don't directly configure (AWS SDK, gRPC, Arrow
  // GCS, Arrow Azure, Snowflake) inherit the same proxy.
  if (auto err = mirror_to_environment(ps)) {
    return err;
  }
  return {};
}

auto get_proxy_settings() -> proxy_settings const& {
  return mutable_proxy_settings();
}

auto effective_no_proxy(proxy_settings const& settings) -> std::string {
  auto result = std::string{
    "localhost,127.0.0.1,127.0.0.0/8,169.254.0.0/16,::1,fe80::/10"};
  if (settings.no_proxy) {
    append_no_proxy_entries(result, *settings.no_proxy);
  }
  return result;
}

namespace {

// Strips IPv6 brackets and interface scopes before comparing IP literals.
auto canonicalize_host(std::string_view host) -> std::string {
  if (host.size() >= 2 and host.front() == '[' and host.back() == ']') {
    host.remove_prefix(1);
    host.remove_suffix(1);
  }
  if (auto scope = host.find('%'); scope != std::string_view::npos) {
    auto unscoped = host.substr(0, scope);
    if (auto address = to<ip>(std::string{unscoped});
        address and address->is_v6()) {
      host = unscoped;
    }
  }
  return to_lower(host);
}

auto is_loopback(std::string_view host) -> bool {
  if (host == "localhost") {
    return true;
  }
  auto address = to<ip>(std::string{host});
  return address and address->is_loopback();
}

auto parse_no_proxy_entry(std::string_view entry) -> std::string_view {
  entry = detail::trim(entry);
  if (entry.empty()) {
    return entry;
  }
  // Bracketed IPv6 host, with optional `:port` suffix.
  if (entry.starts_with('[')) {
    auto end = entry.find(']');
    if (end != std::string_view::npos) {
      return entry.substr(1, end - 1);
    }
    return entry;
  }
  // Unbracketed entries: an entry with more than one `:` is an IPv6
  // address (which doesn't carry a port in this form — RFC 3986 requires
  // brackets for that). Leaving it untouched is correct.
  auto colon_count = std::ranges::count(entry, ':');
  if (colon_count != 1) {
    return entry;
  }
  // For plain `host:port` entries strip the `:port` suffix when the
  // suffix is purely numeric. We ignore the port itself because
  // `bypass_proxy` is only given a host; a proxy decision based on the
  // bare hostname is the closest match libcurl-style entries permit.
  auto colon = entry.rfind(':');
  if (colon != std::string_view::npos and colon + 1 < entry.size()) {
    auto port = entry.substr(colon + 1);
    auto all_digits = std::ranges::all_of(port, [](unsigned char c) {
      return std::isdigit(c) != 0;
    });
    if (all_digits) {
      return entry.substr(0, colon);
    }
  }
  return entry;
}

} // namespace

auto bypass_proxy(std::string_view host) -> bool {
  auto const& ps = get_proxy_settings();
  auto normalized = canonicalize_host(host);
  if (is_loopback(normalized)) {
    return true;
  }
  if (not ps.no_proxy) {
    return false;
  }
  auto host_address = to<ip>(normalized);
  for (auto part : std::views::split(*ps.no_proxy, ',')) {
    auto raw = std::string_view{part.begin(), part.end()};
    auto entry = parse_no_proxy_entry(raw);
    if (entry.empty()) {
      continue;
    }
    if (entry == "*") {
      return true;
    }
    if (entry.contains('/')) {
      auto network = to<subnet>(std::string{entry});
      if (host_address and network and network->contains(*host_address)) {
        return true;
      }
      continue;
    }
    auto normalized_entry = canonicalize_host(entry);
    if (normalized_entry.starts_with('.')) {
      normalized_entry.erase(0, 1);
    }
    if (normalized == normalized_entry
        or normalized.ends_with(fmt::format(".{}", normalized_entry))) {
      return true;
    }
  }
  return false;
}

auto proxy_for_target(std::string_view target_scheme, std::string_view host)
  -> Option<proxy_url> {
  auto const& ps = get_proxy_settings();
  if (bypass_proxy(host)) {
    return {};
  }
  auto normalized_scheme = to_lower(target_scheme);
  if (normalized_scheme == "http") {
    return ps.http_proxy;
  }
  if (normalized_scheme == "https") {
    return ps.https_proxy;
  }
  return {};
}

} // namespace tenzir
