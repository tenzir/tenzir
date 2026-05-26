//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/proxy_settings.hpp"

#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/error.hpp"
#include "tenzir/ip.hpp"

#include <arrow/status.h>
#include <arrow/util/uri.h>
#include <caf/settings.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
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

// Picks a proxy URL from env vars when YAML does not provide one.
// We use HTTPS_PROXY first because most outbound traffic in Tenzir is
// HTTPS (S3, GCS, Azure, Elasticsearch); HTTP_PROXY is the fallback.
auto proxy_from_env() -> Option<std::string> {
  for (auto const* name :
       {"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"}) {
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

// Validates the proxy URL and splits it into its components. The URL
// must parse and must have an explicit port: a missing port would
// fall back to libcurl's port-1080 default (a SOCKS port), which is
// almost certainly not what the user intended.
auto parse_proxy_url(std::string const& url, proxy_settings& out)
  -> caf::error {
  auto parsed = arrow::util::Uri{};
  if (auto status = parsed.Parse(url); not status.ok()) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("tenzir.http-proxy is not a valid URL: "
                                       "{}",
                                       status.ToStringWithoutContextLines()));
  }
  auto scheme = parsed.scheme();
  if (scheme != "http" and scheme != "https") {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("tenzir.http-proxy has unsupported "
                                       "scheme `{}`; expected "
                                       "http or https",
                                       scheme));
  }
  auto host = parsed.host();
  if (host.empty()) {
    return caf::make_error(ec::invalid_configuration,
                           "tenzir.http-proxy is missing a host");
  }
  auto port_text = parsed.port_text();
  if (port_text.empty()) {
    return caf::make_error(ec::invalid_configuration,
                           "tenzir.http-proxy must include an explicit port "
                           "(e.g. http://proxy.example.com:3128)");
  }
  auto port = 0u;
  try {
    port = static_cast<unsigned>(std::stoul(port_text));
  } catch (std::exception const& e) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("tenzir.http-proxy has invalid port "
                                       "`{}`: {}",
                                       port_text, e.what()));
  }
  if (port == 0 or port > 65535) {
    return caf::make_error(
      ec::invalid_configuration,
      fmt::format("tenzir.http-proxy port `{}` is out of range", port_text));
  }
  out.http_proxy = url;
  out.proxy_scheme = std::move(scheme);
  out.proxy_host = std::move(host);
  out.proxy_port = static_cast<uint16_t>(port);
  if (auto user = parsed.username(); not user.empty()) {
    out.proxy_username = std::move(user);
  }
  if (auto pw = parsed.password(); not pw.empty()) {
    out.proxy_password = std::move(pw);
  }
  return {};
}

// Mirrors the resolved proxy back into the process environment. Called
// exactly once from main() while still single-threaded, so the classic
// glibc setenv/getenv race does not apply: no other thread exists yet.
// libcurl-backed cloud SDKs (Arrow GCS, Arrow Azure, Snowflake ADBC,
// the platform WebSocket client) read these env vars on first use.
auto mirror_to_environment(proxy_settings const& ps) -> caf::error {
  auto set = [](char const* name, std::string const& value) -> caf::error {
    if (auto err = detail::setenv(name, value, /*overwrite=*/1)) {
      return err;
    }
    return {};
  };
  if (ps.http_proxy) {
    for (auto const* name :
         {"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"}) {
      if (auto err = set(name, *ps.http_proxy)) {
        return err;
      }
    }
  }
  if (ps.http_proxy or ps.no_proxy) {
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

auto initialize_proxy_settings(caf::settings const& merged_options)
  -> caf::error {
  auto& ps = mutable_proxy_settings();
  ps = proxy_settings{};
  // YAML config wins over env. Read the merged settings first, fall
  // back to env vars only if the config keys are absent or empty.
  auto const* yaml_proxy
    = caf::get_if<std::string>(&merged_options, "tenzir.http-proxy");
  auto const* yaml_no_proxy
    = caf::get_if<std::string>(&merged_options, "tenzir.no-proxy");
  auto resolved_proxy = [&]() -> Option<std::string> {
    if (yaml_proxy and not yaml_proxy->empty()) {
      return Option<std::string>{*yaml_proxy};
    }
    return proxy_from_env();
  }();
  auto resolved_no_proxy = [&]() -> Option<std::string> {
    if (yaml_no_proxy and not yaml_no_proxy->empty()) {
      return Option<std::string>{*yaml_no_proxy};
    }
    return no_proxy_from_env();
  }();
  if (resolved_proxy) {
    if (auto err = parse_proxy_url(*resolved_proxy, ps)) {
      return err;
    }
  }
  if (resolved_no_proxy) {
    ps.no_proxy = std::move(resolved_no_proxy);
  }
  // Mirror to env so libcurl-backed SDKs we don't directly touch
  // (Arrow GCS, Arrow Azure, Snowflake) inherit the same proxy.
  if (auto err = mirror_to_environment(ps)) {
    return err;
  }
  return {};
}

auto get_proxy_settings() -> proxy_settings const& {
  return mutable_proxy_settings();
}

auto effective_no_proxy(proxy_settings const& settings) -> std::string {
  auto result = std::string{"localhost,127.0.0.1,127.0.0.0/8,::1"};
  if (settings.no_proxy) {
    append_no_proxy_entries(result, *settings.no_proxy);
  }
  return result;
}

namespace {

// Strips IPv6 brackets so `[::1]` and `::1` compare equal.
auto canonicalize_host(std::string_view host) -> std::string {
  if (host.size() >= 2 and host.front() == '[' and host.back() == ']') {
    host.remove_prefix(1);
    host.remove_suffix(1);
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
  for (auto part : std::views::split(*ps.no_proxy, ',')) {
    auto raw = std::string_view{part.begin(), part.end()};
    auto entry = parse_no_proxy_entry(raw);
    if (entry.empty()) {
      continue;
    }
    if (entry == "*") {
      return true;
    }
    auto normalized_entry = to_lower(entry);
    auto match_subdomains = false;
    if (normalized_entry.starts_with('.')) {
      normalized_entry.erase(0, 1);
      match_subdomains = true;
    }
    if (normalized == normalized_entry
        or (match_subdomains
            and normalized.ends_with(fmt::format(".{}", normalized_entry)))) {
      return true;
    }
  }
  return false;
}

auto proxy_for_host(std::string_view host) -> Option<std::string> {
  auto const& ps = get_proxy_settings();
  if (not ps.http_proxy) {
    return {};
  }
  if (bypass_proxy(host)) {
    return {};
  }
  return Option<std::string>{*ps.http_proxy};
}

auto proxy_for_grpc_target(std::string_view target) -> Option<std::string> {
  // Bracketed IPv6 literal — `[host]:port`.
  if (target.starts_with('[')) {
    auto close = target.find(']');
    if (close != std::string_view::npos) {
      return proxy_for_host(target.substr(1, close - 1));
    }
    return proxy_for_host(target);
  }
  // Plain `host:port` — strip the rightmost `:N` group when N is
  // numeric. Anything else (unix paths, dns:/// URIs) gets passed
  // through to `proxy_for_host` as-is; the bypass check might miss
  // but the proxy URL itself still gets applied.
  auto colon = target.rfind(':');
  if (colon != std::string_view::npos and colon + 1 < target.size()) {
    auto port = target.substr(colon + 1);
    auto all_digits = std::ranges::all_of(port, [](unsigned char c) {
      return std::isdigit(c) != 0;
    });
    if (all_digits) {
      return proxy_for_host(target.substr(0, colon));
    }
  }
  return proxy_for_host(target);
}

} // namespace tenzir
