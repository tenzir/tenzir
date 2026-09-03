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
#include "tenzir/http.hpp"
#include "tenzir/location.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/web_identity.hpp"

#include <caf/expected.hpp>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace Azure::Core::Credentials {

class TokenCredential;

} // namespace Azure::Core::Credentials

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
  Option<resolved_web_identity> web_identity;
};

/// The parts of an `azure_auth` record an operator can act on.
struct AzureAuthSupport {
  /// `AzureTokenProvider` only implements the client-credentials grant, so
  /// operators that mint tokens through it cannot take a client assertion.
  bool web_identity = true;
  /// Operators that hand a credential to the Azure SDK never see the scope;
  /// the SDK requests it on its own.
  bool scope = true;
};

/// Microsoft/Azure authentication options.
///
/// New operators expose this as a provider-prefixed `azure_auth` record.
/// Operators that already shipped a bare `auth` record keep it.
struct AzureAuthOptions {
  Option<secret> tenant_id;
  Option<secret> client_id;
  Option<secret> client_secret;
  Option<secret> scope;
  Option<secret> authority;
  Option<web_identity_options> web_identity;
  location loc;

  friend auto inspect(auto& f, AzureAuthOptions& x) -> bool {
    return f.object(x).fields(
      f.field("tenant_id", x.tenant_id), f.field("client_id", x.client_id),
      f.field("client_secret", x.client_secret), f.field("scope", x.scope),
      f.field("authority", x.authority),
      f.field("web_identity", x.web_identity), f.field("loc", x.loc));
  }

  /// Parses Azure auth options from a TQL record.
  ///
  /// Recognized keys:
  /// - `tenant_id`: Microsoft Entra tenant ID or domain.
  /// - `client_id`: Application/client ID.
  /// - `client_secret`: Client secret.
  /// - `web_identity`: Federated OIDC token used as a client assertion.
  /// - `scope`: OAuth scope. Defaults are operator-specific.
  /// - `authority`: OAuth authority. Defaults to
  ///   `https://login.microsoftonline.com`.
  ///
  /// Exactly one of `client_secret` and `web_identity` must be present, and
  /// keys the operator does not support per `support` are rejected.
  static auto from_record(located<record> config, diagnostic_handler& dh,
                          AzureAuthSupport support = {})
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

/// Builds an Azure SDK token credential from resolved auth options.
///
/// The Azure SDK owns token caching, refresh and expiry.
auto make_azure_token_credential(ResolvedAzureAuth const& auth)
  -> caf::expected<std::shared_ptr<Azure::Core::Credentials::TokenCredential>>;

/// Lazily fetches and refreshes Microsoft Entra ID OAuth access tokens.
///
/// Only implements the client-credentials grant; parse the options with
/// `AzureAuthSupport{.web_identity = false}`.
class AzureTokenProvider {
public:
  AzureTokenProvider(ResolvedAzureAuth auth, location loc);

  /// Adds an `Authorization: Bearer ...` header, refreshing the token if needed.
  auto authorize(std::vector<http::Header>& headers, OpCtx& ctx,
                 HttpPoolConfig const& config) -> Task<failure_or<void>>;

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
