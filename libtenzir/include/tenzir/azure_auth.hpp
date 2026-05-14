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
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"

#include <chrono>
#include <map>
#include <string>
#include <vector>

namespace tenzir {

class OpCtx;
struct HttpPoolConfig;

/// Resolved Microsoft Entra ID OAuth client-credentials configuration.
struct ResolvedAzureAuth {
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
struct AzureAuthOptions {
  Option<secret> tenant_id;
  Option<secret> client_id;
  Option<secret> client_secret;
  Option<secret> scope;
  Option<secret> authority;
  location loc;

  friend auto inspect(auto& f, AzureAuthOptions& x) -> bool {
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
    -> failure_or<AzureAuthOptions>;

  /// Creates secret requests for resolving credentials.
  auto
  make_secret_requests(ResolvedAzureAuth& resolved, std::string default_scope,
                       diagnostic_handler& dh) const
    -> std::vector<secret_request>;
};

/// Resolves already-parsed Azure auth options through an operator context.
auto resolve_azure_auth(AzureAuthOptions options, std::string default_scope,
                        OpCtx& ctx) -> Task<Option<ResolvedAzureAuth>>;

/// Lazily fetches and refreshes Microsoft Entra ID OAuth access tokens.
class AzureTokenProvider {
public:
  AzureTokenProvider(ResolvedAzureAuth auth, location loc);

  /// Adds an `Authorization: Bearer ...` header, refreshing the token if needed.
  auto authorize(std::map<std::string, std::string>& headers, OpCtx& ctx,
                 HttpPoolConfig const& config) -> Task<failure_or<void>>;

  /// Returns a valid bearer token, refreshing it if needed.
  auto token(OpCtx& ctx, HttpPoolConfig const& config)
    -> Task<failure_or<std::string>>;

private:
  auto refresh(OpCtx& ctx, HttpPoolConfig const& config)
    -> Task<Result<void, diagnostic>>;

  ResolvedAzureAuth auth_;
  location loc_ = location::unknown;
  std::string token_;
  std::chrono::steady_clock::time_point refresh_at_{};
  std::chrono::steady_clock::time_point expires_at_{};
};

} // namespace tenzir
