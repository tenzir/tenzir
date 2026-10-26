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
#include "tenzir/startup_argument.hpp"
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
  Option<nova::Secret> tenant_id;
  Option<nova::Secret> client_id;
  Option<nova::Secret> client_secret;
  Option<nova::Secret> scope;
  Option<nova::Secret> authority;
  Option<web_identity_options> web_identity;
  location loc;
  /// Operator argument name used in diagnostics.
  std::string argument_name = "auth";

  /// Parses Azure auth options from an evaluated TQL record.
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
  static auto
  from_record(located<nova::Record> config, diagnostic_handler& dh,
              AzureAuthSupport support = {}, std::string argument_name = "auth")
    -> failure_or<AzureAuthOptions>;

  /// Evaluates the auth argument at startup and parses it like `from_record`.
  static auto from_argument(StartupArgument<nova::Record> const& config,
                            OpCtx& ctx, AzureAuthSupport support = {},
                            std::string argument_name = "auth")
    -> Task<failure_or<AzureAuthOptions>>;
};

/// Reveals parsed Azure auth options, applying defaults and validating them.
auto resolve_azure_auth(AzureAuthOptions options, std::string default_scope,
                        diagnostic_handler& dh)
  -> failure_or<ResolvedAzureAuth>;

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
