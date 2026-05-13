//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/result.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"

#include <chrono>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace tenzir {

class OpCtx;

/// Resolved Microsoft Entra ID OAuth client-credentials configuration.
struct resolved_azure_auth {
  std::string tenant_id;
  std::string client_id;
  std::string client_secret;
  std::string scope;
  std::string authority;
};

/// Microsoft/Azure authentication options.
///
/// Operators that are already Microsoft/Azure-specific expose this as an
/// `auth` record. The shared implementation keeps the provider-specific name
/// to make reuse across such operators explicit.
struct azure_auth_options {
  std::optional<secret> tenant_id;
  std::optional<secret> client_id;
  std::optional<secret> client_secret;
  std::optional<secret> scope;
  std::optional<secret> authority;
  location loc;

  friend auto inspect(auto& f, azure_auth_options& x) -> bool {
    return f.object(x).fields(
      f.field("tenant_id", x.tenant_id), f.field("client_id", x.client_id),
      f.field("client_secret", x.client_secret), f.field("scope", x.scope),
      f.field("authority", x.authority), f.field("loc", x.loc));
  }

  /// Parses Azure auth options from a TQL record.
  ///
  /// Recognized keys:
  /// - `tenant_id`: Microsoft Entra tenant ID or domain.
  /// - `client_id`: Application/client ID.
  /// - `client_secret`: Client secret.
  /// - `scope`: OAuth scope. Defaults are operator-specific.
  /// - `authority`: OAuth authority. Defaults to
  ///   `https://login.microsoftonline.com`.
  static auto from_record(located<record> config, diagnostic_handler& dh)
    -> failure_or<azure_auth_options>;

  /// Creates secret requests for resolving credentials.
  auto
  make_secret_requests(resolved_azure_auth& resolved, std::string default_scope,
                       diagnostic_handler& dh) const
    -> std::vector<secret_request>;
};

/// Resolves already-parsed Azure auth options through an operator context.
auto resolve_azure_auth(azure_auth_options options, std::string default_scope,
                        OpCtx& ctx) -> Task<std::optional<resolved_azure_auth>>;

/// Lazily fetches and refreshes Microsoft Entra ID OAuth access tokens.
class AzureTokenProvider {
public:
  AzureTokenProvider(resolved_azure_auth auth, location loc);

  /// Adds an `Authorization: Bearer ...` header, refreshing the token if needed.
  auto authorize(std::map<std::string, std::string>& headers, OpCtx& ctx)
    -> Task<failure_or<void>>;

  /// Returns a valid bearer token, refreshing it if needed.
  auto token(OpCtx& ctx) -> Task<failure_or<std::string>>;

private:
  auto refresh(OpCtx& ctx) -> Task<Result<void, diagnostic>>;

  resolved_azure_auth auth_;
  location loc_ = location::unknown;
  std::string token_;
  std::chrono::steady_clock::time_point refresh_at_{};
  std::chrono::steady_clock::time_point expires_at_{};
};

} // namespace tenzir
