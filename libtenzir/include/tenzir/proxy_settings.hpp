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

/// Process-wide proxy configuration resolved from `tenzir.http-proxy` /
/// `tenzir.no-proxy` (with HTTPS_PROXY / HTTP_PROXY / NO_PROXY env var
/// fallback). Populated exactly once by `initialize_proxy_settings`.
struct proxy_settings {
  /// Original URL as provided by the user, including userinfo. Consumed
  /// as-is by libcurl (`CURLOPT_PROXY`), AWS SDK
  /// (`S3ProxyOptions::FromUri`), Arrow S3, and gRPC
  /// (`grpc.http_proxy` channel argument).
  Option<std::string> http_proxy;

  /// Convenience-split fields for SDKs and chokepoints that need individual
  /// proxy URL components.
  Option<std::string> proxy_host;
  Option<uint16_t> proxy_port;
  /// "http" or "https" — the proxy itself, not the target.
  Option<std::string> proxy_scheme;
  Option<std::string> proxy_username;
  Option<std::string> proxy_password;

  /// User-provided comma-separated host list matched by `bypass_proxy`.
  Option<std::string> no_proxy;
};

/// Initialises the process-wide proxy settings from the merged Tenzir
/// configuration. Must be called exactly once from `main()` while still
/// single-threaded — before spdlog spawns its async worker, before CAF
/// starts the actor system, and before any operator constructs an HTTP
/// client. This is the only point at which Tenzir calls `setenv`.
///
/// Resolution order: YAML config wins over env vars. If both are
/// absent, no proxy is configured. After resolving, the function
/// mirrors the result back into the process environment
/// (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` and lowercase variants)
/// so libcurl-backed cloud SDKs (Arrow GCS, Arrow Azure, Snowflake
/// ADBC) see the same proxy without the user touching env vars.
///
/// The proxy URL is validated: it must parse and include an explicit
/// port. A malformed URL aborts startup with a clear error — a typo in
/// YAML must not silently route traffic direct.
[[nodiscard]] auto
initialize_proxy_settings(const caf::settings& merged_options) -> caf::error;

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
/// Semantics match libcurl's `CURLOPT_NOPROXY` plus the platform
/// plugin's `no_proxy_matches_host`: comma-separated entries; leading
/// "." for suffix match; exact host match; `*` matches every host;
/// `localhost` and IPv4/IPv6 loopback always bypass. IPv6 brackets
/// in `host` are stripped before comparison.
auto bypass_proxy(std::string_view host) -> bool;

/// Returns the resolved proxy URL when one is configured and `host`
/// is not on the bypass list. Convenience for chokepoints that branch
/// on "is there a proxy for this target host".
auto proxy_for_host(std::string_view host) -> Option<std::string>;

/// Same as `proxy_for_host` but takes a gRPC target string in the
/// canonical `host:port` form (or `[ipv6]:port`). Extracts the host
/// portion before consulting the bypass list. Schemes like
/// `dns:///host:port` or `unix:path` are passed through to
/// `proxy_for_host` unchanged — the caller should ensure the gRPC
/// target is in a supported form.
auto proxy_for_grpc_target(std::string_view target) -> Option<std::string>;

} // namespace tenzir
