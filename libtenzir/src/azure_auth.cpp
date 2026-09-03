//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/azure_auth.hpp"

#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"

#include <arrow/util/config.h>
#include <boost/url/parse.hpp>
#include <folly/io/async/EventBase.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <iterator>

#ifdef ARROW_AZURE
#  include <azure/core/context.hpp>
#  include <azure/core/credentials/credentials.hpp>
#  include <azure/identity/client_assertion_credential.hpp>
#  include <azure/identity/client_secret_credential.hpp>
#endif

namespace tenzir {

namespace {

using namespace std::chrono_literals;

constexpr auto default_authority = "https://login.microsoftonline.com";

/// Fallback token lifetime for Azure token responses without a usable
/// `expires_in` field.
constexpr auto default_token_lifetime = 50min;

auto check_resolved(std::string_view name, std::string const& value,
                    location loc, diagnostic_handler& dh) -> failure_or<void> {
  if (value.empty()) {
    diagnostic::error("`auth.{}` must not be empty", name).primary(loc).emit(dh);
    return failure::promise();
  }
  return {};
}

auto normalize_authority(std::string authority) -> std::string {
  while (authority.ends_with('/')) {
    authority.pop_back();
  }
  return authority;
}

auto validate_authority(std::string_view authority, location loc,
                        diagnostic_handler& dh) -> failure_or<void> {
  auto parsed = boost::urls::parse_uri(authority);
  if (not parsed) {
    diagnostic::error("invalid `auth.authority` URL: {}",
                      parsed.error().message())
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  if (parsed->scheme() != "https" and parsed->scheme() != "http") {
    diagnostic::error("`auth.authority` must use HTTP or HTTPS")
      .primary(loc)
      .note("scheme: {}", parsed->scheme())
      .emit(dh);
    return failure::promise();
  }
  if (parsed->host().empty()) {
    diagnostic::error("`auth.authority` must include a host")
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto token_endpoint(ResolvedAzureAuth const& auth) -> std::string {
  return fmt::format("{}/{}/oauth2/v2.0/token", auth.authority, auth.tenant_id);
}

auto parse_expires_in(record const& response) -> std::chrono::seconds {
  auto it = response.find("expires_in");
  if (it == response.end()) {
    return default_token_lifetime;
  }
  auto seconds = uint64_t{};
  if (auto* value = try_as<uint64_t>(it->second)) {
    seconds = *value;
  } else if (auto* value = try_as<int64_t>(it->second); value and *value > 0) {
    seconds = static_cast<uint64_t>(*value);
  } else if (auto* value = try_as<double>(it->second); value and *value > 0) {
    seconds = static_cast<uint64_t>(*value);
  } else {
    return default_token_lifetime;
  }
  return std::chrono::seconds{seconds};
}

auto refresh_time(std::chrono::steady_clock::time_point now,
                  std::chrono::seconds expires_in)
  -> std::chrono::steady_clock::time_point {
  auto safety_margin = 60s;
  if (expires_in <= 2 * safety_margin) {
    safety_margin = expires_in / 2;
  }
  return now + expires_in - safety_margin;
}

/// The SDK expects a trailing slash on the authority host, but
/// `resolve_azure_auth` strips it, so this helper adds it back.
template <class Options>
auto credential_options(ResolvedAzureAuth const& auth) -> Options {
  auto options = Options{};
  options.AuthorityHost = auth.authority + "/";
  return options;
}

} // namespace

auto AzureAuthOptions::from_record(located<record> config,
                                   diagnostic_handler& dh,
                                   AzureAuthSupport support)
  -> failure_or<AzureAuthOptions> {
  constexpr auto known = std::array{
    "tenant_id", "client_id", "client_secret",
    "scope",     "authority", "web_identity",
  };
  auto const unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in `auth`", (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = AzureAuthOptions{};
  opts.loc = config.source;
  TRY(assign_secret(config, "tenant_id", opts.tenant_id, dh));
  TRY(assign_secret(config, "client_id", opts.client_id, dh));
  TRY(assign_secret(config, "client_secret", opts.client_secret, dh));
  TRY(assign_secret(config, "scope", opts.scope, dh));
  TRY(assign_secret(config, "authority", opts.authority, dh));
  if (auto it = config.inner.find("web_identity"); it != config.inner.end()) {
    if (auto* r = try_as<record>(it->second.get_data())) {
      TRY(opts.web_identity, web_identity_options::from_record(
                               located{std::move(*r), config.source}, dh));
    } else {
      diagnostic::error("`web_identity` must be a record")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  if (not opts.tenant_id) {
    diagnostic::error("`auth` requires `tenant_id`").primary(config).emit(dh);
    return failure::promise();
  }
  if (not opts.client_id) {
    diagnostic::error("`auth` requires `client_id`").primary(config).emit(dh);
    return failure::promise();
  }
  if (opts.client_secret and opts.web_identity) {
    diagnostic::error("`client_secret` and `web_identity` are mutually "
                      "exclusive")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  if (not opts.client_secret and not opts.web_identity) {
    diagnostic::error("`auth` requires one of: `client_secret`, "
                      "`web_identity`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  if (opts.web_identity and not support.web_identity) {
    diagnostic::error("`web_identity` is not supported by this operator")
      .primary(config)
      .hint("use `client_secret` instead")
      .emit(dh);
    return failure::promise();
  }
  if (opts.scope and not support.scope) {
    diagnostic::error("`scope` is not supported by this operator")
      .primary(config)
      .note("the Azure SDK requests the scope itself")
      .emit(dh);
    return failure::promise();
  }
  return opts;
}

auto AzureAuthOptions::make_secret_requests(ResolvedAzureAuth& resolved,
                                            std::string default_scope,
                                            diagnostic_handler& dh) const
  -> std::vector<secret_request> {
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(make_secret_request("auth.tenant_id", *tenant_id, loc,
                                            resolved.tenant_id, dh));
  requests.emplace_back(make_secret_request("auth.client_id", *client_id, loc,
                                            resolved.client_id, dh));
  if (client_secret) {
    requests.emplace_back(make_secret_request(
      "auth.client_secret", *client_secret, loc, resolved.client_secret, dh));
  }
  if (web_identity) {
    resolved.web_identity = resolved_web_identity{};
    auto web_identity_requests
      = web_identity->make_secret_requests(*resolved.web_identity, dh);
    requests.insert(requests.end(),
                    std::make_move_iterator(web_identity_requests.begin()),
                    std::make_move_iterator(web_identity_requests.end()));
  }
  if (scope) {
    requests.emplace_back(
      make_secret_request("auth.scope", *scope, loc, resolved.scope, dh));
  } else {
    resolved.scope = std::move(default_scope);
  }
  if (authority) {
    requests.emplace_back(make_secret_request("auth.authority", *authority, loc,
                                              resolved.authority, dh));
  } else {
    resolved.authority = default_authority;
  }
  return requests;
}

auto resolve_azure_auth(AzureAuthOptions options, std::string default_scope,
                        OpCtx& ctx) -> Task<Option<ResolvedAzureAuth>> {
  auto resolved = ResolvedAzureAuth{};
  auto requests = options.make_secret_requests(
    resolved, std::move(default_scope), ctx.dh());
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return None{};
  }
  auto& dh = ctx.dh();
  if (not check_resolved("tenant_id", resolved.tenant_id, options.loc, dh)
      or not check_resolved("client_id", resolved.client_id, options.loc, dh)
      or not check_resolved("scope", resolved.scope, options.loc, dh)
      or not check_resolved("authority", resolved.authority, options.loc, dh)) {
    co_return None{};
  }
  if (options.client_secret
      and not check_resolved("client_secret", resolved.client_secret,
                             options.loc, dh)) {
    co_return None{};
  }
  resolved.authority = normalize_authority(std::move(resolved.authority));
  if (not validate_authority(resolved.authority, options.loc, dh)) {
    co_return None{};
  }
  co_return resolved;
}

auto make_azure_token_credential(ResolvedAzureAuth const& auth)
  -> caf::expected<std::shared_ptr<Azure::Core::Credentials::TokenCredential>> {
#ifdef ARROW_AZURE
  using credential_ptr
    = std::shared_ptr<Azure::Core::Credentials::TokenCredential>;
  if (not auth.web_identity) {
    return credential_ptr{
      std::make_shared<Azure::Identity::ClientSecretCredential>(
        auth.tenant_id, auth.client_id, auth.client_secret,
        credential_options<Azure::Identity::ClientSecretCredentialOptions>(
          auth))};
  }
  auto options
    = credential_options<Azure::Identity::ClientAssertionCredentialOptions>(
      auth);
  auto callback
    = [web_identity = *auth.web_identity](Azure::Core::Context const&) {
        auto token = fetch_web_identity_token(web_identity);
        if (not token) {
          throw Azure::Core::Credentials::AuthenticationException{
            fmt::format("failed to fetch client assertion: {}", token.error())};
        }
        return std::move(*token);
      };
  return credential_ptr{
    std::make_shared<Azure::Identity::ClientAssertionCredential>(
      auth.tenant_id, auth.client_id, std::move(callback), options)};
#else
  (void)auth;
  return diagnostic::error("Azure support is not available in this build")
    .to_error();
#endif
}

AzureTokenProvider::AzureTokenProvider(ResolvedAzureAuth auth, location loc)
  : auth_{std::move(auth)}, loc_{loc} {
  TENZIR_ASSERT(not auth_.web_identity);
}

auto AzureTokenProvider::authorize(std::vector<http::Header>& headers,
                                   OpCtx& ctx, HttpPoolConfig const& config)
  -> Task<failure_or<void>> {
  CO_TRY(auto token, co_await this->token(ctx, config));
  http::set(headers, "Authorization", fmt::format("Bearer {}", token));
  co_return {};
}

auto AzureTokenProvider::authorize(std::map<std::string, std::string>& headers,
                                   OpCtx& ctx, HttpPoolConfig const& config)
  -> Task<failure_or<void>> {
  CO_TRY(auto token, co_await this->token(ctx, config));
  headers["Authorization"] = fmt::format("Bearer {}", token);
  co_return {};
}

auto AzureTokenProvider::token(OpCtx& ctx, HttpPoolConfig const& config)
  -> Task<failure_or<std::string>> {
  auto const now = std::chrono::steady_clock::now();
  if (not token_.empty() and now < refresh_at_) {
    co_return token_;
  }
  auto result = co_await refresh(ctx, config);
  if (result.is_err()) {
    auto diag = std::move(result).unwrap_err();
    if (not token_.empty() and std::chrono::steady_clock::now() < expires_at_) {
      std::move(diag)
        .modify()
        .severity(severity::warning)
        .note("continuing with the cached token until it expires")
        .emit(ctx.dh());
      co_return token_;
    }
    ctx.dh().emit(std::move(diag));
    co_return failure::promise();
  }
  co_return token_;
}

auto AzureTokenProvider::refresh(OpCtx& ctx, HttpPoolConfig const& config)
  -> Task<Result<void, diagnostic>> {
  auto const url = token_endpoint(auth_);
  auto const body = curl::escape(record{
    {"client_id", auth_.client_id},
    {"client_secret", auth_.client_secret},
    {"grant_type", "client_credentials"},
    {"scope", auth_.scope},
  });
  auto headers = std::vector<http::Header>{
    {"Content-Type", "application/x-www-form-urlencoded"},
    {"Content-Length", fmt::to_string(body.size())},
  };
  auto token_config = config;
  token_config.tls = url.starts_with("https://");
  if (not token_config.tls) {
    token_config.ssl_context.reset();
  }
  auto pool = HttpPool::make(ctx.io_executor(), url, std::move(token_config));
  auto result = co_await pool->post(body, std::move(headers));
  if (result.is_err()) {
    co_return Err{diagnostic::error("failed to fetch Azure access token: {}",
                                    std::move(result).unwrap_err())
                    .primary(loc_)
                    .done()};
  }
  auto response = std::move(result).unwrap();
  auto fail = [&](std::string message) -> Result<void, diagnostic> {
    return Err{diagnostic_builder{severity::error, std::move(message)}
                 .primary(loc_)
                 .note("response body: {}", response.body)
                 .done()};
  };
  if (not response.is_status_success()) {
    co_return fail(
      fmt::format("failed to fetch Azure access token: HTTP code `{}`",
                  response.status_code));
  }
  auto const json = from_json(response.body);
  if (not json.has_value()) {
    co_return fail("received no JSON when fetching Azure access token");
  }
  auto const* object = try_as<record>(json.value());
  if (not object) {
    co_return fail("Azure token response body is not a JSON object");
  }
  auto const it = object->find("access_token");
  if (it == object->end()) {
    co_return fail("Azure token response does not contain `access_token`");
  }
  auto const* token = try_as<std::string>(it->second);
  if (not token) {
    co_return fail(
      fmt::format("expected Azure `access_token` to be `string`, got `{}`",
                  type::infer(it->second).value_or(type{}).kind()));
  }
  auto const expires_in = parse_expires_in(*object);
  auto const now = std::chrono::steady_clock::now();
  token_ = *token;
  refresh_at_ = refresh_time(now, expires_in);
  expires_at_ = now + expires_in;
  co_return {};
}

} // namespace tenzir
