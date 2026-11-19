//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async/task.hpp>
#include <tenzir/result.hpp>

#include <chrono>
#include <string>
#include <string_view>

namespace tenzir {

/// Sent with every request a deployment makes to the platform.
inline constexpr auto deployment_key_header
  = std::string_view{"x-tenzir-deployment-key"};
inline constexpr auto deployment_version_header
  = std::string_view{"x-tenzir-deployment-version"};

/// The defaults suit the deployment; best-effort telemetry passes shorter ones.
struct DiscoveryTimeouts {
  std::chrono::milliseconds connect = std::chrono::seconds{15};
  std::chrono::milliseconds total = std::chrono::seconds{30};
};

auto discovery_url(std::string_view platform_url)
  -> Result<std::string, std::string>;

auto gateway_url_from_configuration(std::string_view body)
  -> Result<std::string, std::string>;

/// Where the platform serves its API, which is where a deployment calls it
/// directly rather than over the tunnel.
auto api_url_from_configuration(std::string_view body)
  -> Result<std::string, std::string>;

auto fetch_platform_configuration(std::string const& platform_url,
                                  DiscoveryTimeouts timeouts = {})
  -> Task<Result<std::string, std::string>>;

auto discover_gateway_url(std::string const& platform_url,
                          DiscoveryTimeouts timeouts = {})
  -> Task<Result<std::string, std::string>>;

auto discover_api_url(std::string const& platform_url,
                      DiscoveryTimeouts timeouts = {})
  -> Task<Result<std::string, std::string>>;

} // namespace tenzir
