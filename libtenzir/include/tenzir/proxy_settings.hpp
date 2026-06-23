//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <caf/error.hpp>
#include <caf/settings.hpp>

#include <cstdint>
#include <string>
#include <string_view>

namespace tenzir {

/// Parsed proxy URL for one target scheme.
struct proxy_url {
  /// Original URL as provided by the user, including userinfo.
  std::string url;
  std::string host;
  uint16_t port = 0;
  /// "http" or "https" -- the proxy itself, not the target.
  std::string scheme;
  Option<std::string> username;
  Option<std::string> password;
};

/// Process-wide proxy configuration resolved from `tenzir.http-proxy` /
/// `tenzir.https-proxy` / `tenzir.no-proxy` (with `TENZIR_HTTP_PROXY`,
/// `TENZIR_HTTPS_PROXY`, `TENZIR_NO_PROXY`, and generic proxy env var
/// fallback). Populated exactly once by `initialize_proxy_settings`.
struct proxy_settings {
  /// Proxy used for `http://` targets.
  Option<proxy_url> http_proxy;
  /// Proxy used for `https://` targets and mirrored to env for gRPC.
  Option<proxy_url> https_proxy;

  /// User-provided comma-separated host list matched by `bypass_proxy`.
  Option<std::string> no_proxy;
};

/// Initialises the process-wide proxy settings from the merged Tenzir
/// configuration. Must be called exactly once from `main()` while still
/// single-threaded — before spdlog spawns its async worker, before CAF
/// starts the actor system, and before any operator constructs an HTTP
/// client. This is the only point at which Tenzir calls `setenv`.
///
/// `options` contains proxy values from Tenzir's configuration sources. Empty
/// proxy URL settings fall back to direct proxy env reads. `tenzir.no-proxy`
/// falls back to env only when unset; an explicit empty value disables
/// user-provided bypass entries. After resolving, the function mirrors the
/// result back into the process environment (`HTTP_PROXY`, `HTTPS_PROXY`,
/// `NO_PROXY` and lowercase variants) so AWS SDK, gRPC, and libcurl-backed
/// cloud SDKs (Arrow GCS, Arrow Azure, Snowflake ADBC) see the same proxy
/// without the user touching env vars.
///
/// The proxy URL is validated: it must parse and include an explicit
/// port. A malformed URL aborts startup with a clear error — a typo in
/// YAML must not silently route traffic direct.
[[nodiscard]] auto initialize_proxy_settings(caf::settings const& options)
  -> caf::error;

/// Returns the resolved proxy settings. Safe to call from any thread
/// after `initialize_proxy_settings` has run. The returned reference
/// is valid for the lifetime of the process.
auto get_proxy_settings() -> const proxy_settings&;

/// Returns the comma-separated no-proxy list handed to backend APIs. This
/// preserves the user-provided `tenzir.no-proxy` list but prepends Tenzir's
/// implicit loopback bypass entries.
auto effective_no_proxy(proxy_settings const& settings) -> std::string;

/// Returns true when `host` matches the `no-proxy` bypass list.
///
/// Semantics match libcurl's `CURLOPT_NOPROXY`: comma-separated entries;
/// domain entries match the hostname itself or subdomains on a label boundary;
/// CIDR entries match IP-literal hosts only; `*` matches every host;
/// `localhost` and IPv4/IPv6 loopback always bypass. IPv6 brackets in `host`
/// are stripped before comparison.
auto bypass_proxy(std::string_view host) -> bool;

/// Returns the resolved proxy URL when one is configured for `target_scheme`
/// and `host` is not on the bypass list. `target_scheme` is the scheme of the
/// outbound request, not the scheme of the proxy URL.
auto proxy_for_target(std::string_view target_scheme, std::string_view host)
  -> Option<proxy_url>;

} // namespace tenzir
