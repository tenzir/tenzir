//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http_auth.hpp"

#include "tenzir/arc.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/location.hpp"
#include "tenzir/option.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"

#include <boost/url/parse.hpp>
#include <caf/actor_system_config.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <ranges>
#include <string>
#include <utility>

namespace tenzir {

namespace {

using namespace std::chrono_literals;

constexpr auto default_token_lifetime = 50min;
constexpr auto default_api_key_header_name = "X-Api-Key";

auto assign_secret(located<record> const& config, std::string_view key,
                   Option<secret>& x, diagnostic_handler& dh)
  -> failure_or<void> {
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* s = try_as<secret>(it->second.get_data())) {
      x = std::move(*s);
    } else if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x = secret::make_literal(std::move(*str));
    } else {
      diagnostic::error("'{}' must be a `string` or `secret`", key)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

auto assign_string(located<record> const& config, std::string_view key,
                   std::string& x, diagnostic_handler& dh) -> failure_or<void> {
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x = std::move(*str);
    } else {
      diagnostic::error("'{}' must be a `string`", key).primary(config).emit(dh);
      return failure::promise();
    }
  }
  return {};
}

auto assign_optional_string(located<record> const& config, std::string_view key,
                            Option<std::string>& x, diagnostic_handler& dh)
  -> failure_or<void> {
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x = std::move(*str);
    } else {
      diagnostic::error("'{}' must be a `string`", key).primary(config).emit(dh);
      return failure::promise();
    }
  }
  return {};
}

auto assign_scopes(located<record> const& config, std::string_view key,
                   std::vector<secret>& out, diagnostic_handler& dh)
  -> failure_or<void> {
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      out.push_back(secret::make_literal(std::move(*str)));
      return {};
    }
    auto const* lst = try_as<list>(it->second.get_data());
    if (not lst) {
      diagnostic::error("'{}' must be a `string` or `list`", key)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    for (auto const& value : *lst) {
      if (auto* s = try_as<secret>(&value)) {
        out.push_back(*s);
      } else if (auto* str = try_as<std::string>(&value)) {
        if (str->empty()) {
          diagnostic::error("scope values must not be empty")
            .primary(config)
            .emit(dh);
          return failure::promise();
        }
        out.push_back(secret::make_literal(*str));
      } else {
        diagnostic::error("scope values must be `string` or `secret`")
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
    }
  }
  return {};
}

auto check_resolved(std::string_view name, std::string const& value,
                    location loc, diagnostic_handler& dh) -> failure_or<void> {
  if (value.empty()) {
    diagnostic::error("`auth.{}` must not be empty", name).primary(loc).emit(dh);
    return failure::promise();
  }
  return {};
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

auto validate_url(std::string_view url, location loc, diagnostic_handler& dh)
  -> failure_or<void> {
  auto parsed = boost::urls::parse_uri(url);
  if (not parsed) {
    diagnostic::error("invalid auth token URL: {}", parsed.error().message())
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  if (parsed->scheme() != "https" and parsed->scheme() != "http") {
    diagnostic::error("auth token URL must use HTTP or HTTPS")
      .primary(loc)
      .note("scheme: {}", parsed->scheme())
      .emit(dh);
    return failure::promise();
  }
  if (parsed->host().empty()) {
    diagnostic::error("auth token URL must include a host")
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto make_token_config(std::string_view url, HttpPoolConfig config)
  -> HttpPoolConfig {
  config.tls = url.starts_with("https://");
  if (not config.tls) {
    config.ssl_context.reset();
  }
  return config;
}

struct OAuthAuthOptions {
  Option<secret> client_id;
  Option<secret> client_secret;
  Option<secret> token_url;
  std::vector<secret> scopes;
  std::string grant_type = "client_credentials";
  Option<std::string> audience;
  location loc = location::unknown;
};

struct ResolvedOAuthAuth {
  std::string client_id;
  std::string client_secret;
  std::string token_url;
  std::string grant_type;
  std::vector<std::string> scopes;
  Option<std::string> audience;
};

struct BasicAuthOptions {
  Option<secret> username;
  Option<secret> password;
  location loc = location::unknown;
};

struct ResolvedBasicAuth {
  std::string username;
  std::string password;
};

struct ApiKeyAuthOptions {
  Option<secret> api_key;
  std::string header_name = default_api_key_header_name;
  location loc = location::unknown;
};

struct ResolvedApiKeyAuth {
  std::string api_key;
  std::string header_name = default_api_key_header_name;
};

struct BearerStaticAuthOptions {
  Option<secret> token;
  location loc = location::unknown;
};

struct ResolvedBearerStaticAuth {
  std::string token;
};

auto parse_oauth_options(located<record> config, diagnostic_handler& dh)
  -> failure_or<OAuthAuthOptions> {
  constexpr auto known = std::array{
    "name",      "strategy", "client_id",  "client_secret",
    "token_url", "scopes",   "grant_type", "audience",
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
  auto opts = OAuthAuthOptions{};
  opts.loc = config.source;
  TRY(assign_secret(config, "client_id", opts.client_id, dh));
  TRY(assign_secret(config, "client_secret", opts.client_secret, dh));
  TRY(assign_secret(config, "token_url", opts.token_url, dh));
  TRY(assign_scopes(config, "scopes", opts.scopes, dh));
  TRY(assign_string(config, "grant_type", opts.grant_type, dh));
  TRY(assign_optional_string(config, "audience", opts.audience, dh));
  if (not opts.client_id) {
    diagnostic::error("`auth` requires `client_id`").primary(config).emit(dh);
    return failure::promise();
  }
  if (not opts.client_secret) {
    diagnostic::error("`auth` requires `client_secret`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  if (not opts.token_url) {
    diagnostic::error("`auth` requires `token_url`").primary(config).emit(dh);
    return failure::promise();
  }
  if (opts.scopes.empty() and not opts.audience) {
    diagnostic::error("`auth` requires non-empty `scopes` or `audience`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  if (opts.grant_type.empty()) {
    diagnostic::error("`auth.grant_type` must not be empty")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  return opts;
}

auto parse_basic_options(located<record> config, diagnostic_handler& dh)
  -> failure_or<BasicAuthOptions> {
  constexpr auto known = std::array{"name", "strategy", "username", "password"};
  auto const unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in `auth`", (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = BasicAuthOptions{};
  opts.loc = config.source;
  TRY(assign_secret(config, "username", opts.username, dh));
  TRY(assign_secret(config, "password", opts.password, dh));
  if (not opts.username) {
    diagnostic::error("`auth` requires `username`").primary(config).emit(dh);
    return failure::promise();
  }
  if (not opts.password) {
    diagnostic::error("`auth` requires `password`").primary(config).emit(dh);
    return failure::promise();
  }
  return opts;
}

auto parse_api_key_options(located<record> config, diagnostic_handler& dh)
  -> failure_or<ApiKeyAuthOptions> {
  constexpr auto known
    = std::array{"name", "strategy", "api_key", "header_name"};
  auto const unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in `auth`", (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = ApiKeyAuthOptions{};
  opts.loc = config.source;
  TRY(assign_secret(config, "api_key", opts.api_key, dh));
  TRY(assign_string(config, "header_name", opts.header_name, dh));
  if (not opts.api_key) {
    diagnostic::error("`auth` requires `api_key`").primary(config).emit(dh);
    return failure::promise();
  }
  if (opts.header_name.empty()) {
    diagnostic::error("`auth.header_name` must not be empty")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  return opts;
}

auto parse_bearer_static_options(located<record> config, diagnostic_handler& dh)
  -> failure_or<BearerStaticAuthOptions> {
  constexpr auto known = std::array{"name", "strategy", "token"};
  auto const unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in `auth`", (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = BearerStaticAuthOptions{};
  opts.loc = config.source;
  TRY(assign_secret(config, "token", opts.token, dh));
  if (not opts.token) {
    diagnostic::error("`auth` requires `token`").primary(config).emit(dh);
    return failure::promise();
  }
  return opts;
}

auto resolve_oauth_auth(OAuthAuthOptions const& options, OpCtx& ctx)
  -> Task<Option<ResolvedOAuthAuth>> {
  auto resolved = ResolvedOAuthAuth{};
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(make_secret_request("auth.client_id",
                                            *options.client_id, options.loc,
                                            resolved.client_id, ctx.dh()));
  requests.emplace_back(make_secret_request("auth.client_secret",
                                            *options.client_secret, options.loc,
                                            resolved.client_secret, ctx.dh()));
  requests.emplace_back(make_secret_request("auth.token_url",
                                            *options.token_url, options.loc,
                                            resolved.token_url, ctx.dh()));
  for (auto const& scope : options.scopes) {
    resolved.scopes.emplace_back();
    requests.emplace_back(make_secret_request(
      "auth.scopes", scope, options.loc, resolved.scopes.back(), ctx.dh()));
  }
  resolved.grant_type = options.grant_type;
  if (options.audience) {
    resolved.audience = *options.audience;
  }
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return None{};
  }
  auto& dh = ctx.dh();
  if (not check_resolved("client_id", resolved.client_id, options.loc, dh)
      or not check_resolved("client_secret", resolved.client_secret,
                            options.loc, dh)
      or not check_resolved("token_url", resolved.token_url, options.loc, dh)
      or not check_resolved("grant_type", resolved.grant_type, options.loc,
                            dh)) {
    co_return None{};
  }
  if (auto valid = validate_url(resolved.token_url, options.loc, dh);
      not valid) {
    co_return None{};
  }
  for (auto const& scope : resolved.scopes) {
    if (scope.empty()) {
      diagnostic::error("`auth.scopes` must not contain empty values")
        .primary(options.loc)
        .emit(dh);
      co_return None{};
    }
  }
  co_return resolved;
}

auto resolve_basic_auth(BasicAuthOptions const& options, OpCtx& ctx)
  -> Task<Option<ResolvedBasicAuth>> {
  auto resolved = ResolvedBasicAuth{};
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(make_secret_request("auth.username", *options.username,
                                            options.loc, resolved.username,
                                            ctx.dh()));
  requests.emplace_back(make_secret_request("auth.password", *options.password,
                                            options.loc, resolved.password,
                                            ctx.dh()));
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return None{};
  }
  auto& dh = ctx.dh();
  if (not check_resolved("username", resolved.username, options.loc, dh)
      or not check_resolved("password", resolved.password, options.loc, dh)) {
    co_return None{};
  }
  co_return resolved;
}

auto resolve_api_key_auth(ApiKeyAuthOptions const& options, OpCtx& ctx)
  -> Task<Option<ResolvedApiKeyAuth>> {
  auto resolved = ResolvedApiKeyAuth{};
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(make_secret_request(
    "auth.api_key", *options.api_key, options.loc, resolved.api_key, ctx.dh()));
  resolved.header_name = options.header_name;
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return None{};
  }
  auto& dh = ctx.dh();
  if (not check_resolved("api_key", resolved.api_key, options.loc, dh)) {
    co_return None{};
  }
  co_return resolved;
}

auto resolve_bearer_static_auth(BearerStaticAuthOptions const& options,
                                OpCtx& ctx)
  -> Task<Option<ResolvedBearerStaticAuth>> {
  auto resolved = ResolvedBearerStaticAuth{};
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(make_secret_request(
    "auth.token", *options.token, options.loc, resolved.token, ctx.dh()));
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return None{};
  }
  auto& dh = ctx.dh();
  if (not check_resolved("token", resolved.token, options.loc, dh)) {
    co_return None{};
  }
  co_return resolved;
}

auto parse_token_response(record const& response, location loc,
                          diagnostic_handler& dh)
  -> failure_or<std::pair<std::string, std::chrono::seconds>> {
  auto const it = response.find("access_token");
  if (it == response.end()) {
    diagnostic::error("token response does not contain `access_token`")
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  auto const* token = try_as<std::string>(it->second);
  if (not token) {
    diagnostic::error("expected token `access_token` to be `string`, got `{}`",
                      type::infer(it->second).value_or(type{}).kind())
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  return std::pair{*token, parse_expires_in(response)};
}

using AuthCacheKey = std::pair<void const*, std::string>;

struct AuthCacheState {
  Option<AuthorizationConfig> authorization;
  Option<ResolvedOAuthAuth> oauth;
  std::string token;
  std::chrono::steady_clock::time_point refresh_at{};
  std::chrono::steady_clock::time_point expires_at{};
};

struct AuthCacheEntry {
  explicit AuthCacheEntry(located<record> config)
    : config{std::move(config)}, state{AuthCacheState{}} {
  }

  located<record> config;
  Mutex<AuthCacheState> state;
};

auto auth_cache() -> Mutex<std::map<AuthCacheKey, Arc<AuthCacheEntry>>>& {
  static auto cache = Mutex<std::map<AuthCacheKey, Arc<AuthCacheEntry>>>{
    std::map<AuthCacheKey, Arc<AuthCacheEntry>>{},
  };
  return cache;
}

auto find_local_auth_entry(std::string_view name, location loc, OpCtx& ctx)
  -> Task<failure_or<Option<located<record>>>> {
  auto const& config = content(ctx.actor_system().config());
  auto const* auth_list
    = caf::get_if<caf::config_value::list>(&config, "tenzir.auth");
  if (auth_list == nullptr) {
    co_return Option<located<record>>{};
  }
  auto found = Option<located<record>>{};
  for (auto const& entry : *auth_list) {
    auto rec = to<data>(entry);
    if (not rec) {
      diagnostic::error("`tenzir.auth` entries must be records")
        .primary(loc)
        .emit(ctx);
      co_return failure::promise();
    }
    auto const* auth = try_as<record>(&*rec);
    if (not auth) {
      diagnostic::error("`tenzir.auth` entries must be records")
        .primary(loc)
        .emit(ctx);
      co_return failure::promise();
    }
    auto const* name_data
      = auth->find("name") != auth->end() ? &auth->at("name") : nullptr;
    auto const* entry_name
      = name_data ? try_as<std::string>(name_data) : nullptr;
    if (not entry_name) {
      diagnostic::error("each `tenzir.auth` entry requires a `name`")
        .primary(loc)
        .emit(ctx);
      co_return failure::promise();
    }
    if (*entry_name == name) {
      if (found) {
        diagnostic::error("duplicate `tenzir.auth` entry for `{}`", name)
          .primary(loc)
          .emit(ctx);
        co_return failure::promise();
      }
      found = located{*auth, location::unknown};
    }
  }
  co_return found;
}

auto fetch_platform_auth_entry(std::string_view name, OpCtx& ctx)
  -> Task<failure_or<located<record>>> {
  auto resolved = co_await ctx.resolve_authentication(std::string{name});
  if (not resolved) {
    co_return failure::promise();
  }
  auto rec = std::move(resolved->fields);
  rec.insert_or_assign("name", std::string{name});
  rec.insert_or_assign("strategy", std::move(resolved->strategy));
  co_return located{std::move(rec), location::unknown};
}

auto find_cached_auth_entry(std::string_view name, location loc, OpCtx& ctx)
  -> Task<failure_or<Arc<AuthCacheEntry>>> {
  auto key = AuthCacheKey{&ctx.actor_system(), std::string{name}};
  {
    auto guard = co_await auth_cache().lock();
    if (auto it = guard->find(key); it != guard->end()) {
      co_return it->second;
    }
  }
  auto local = co_await find_local_auth_entry(name, loc, ctx);
  if (not local) {
    co_return failure::promise();
  }
  auto config = located<record>{};
  if (*local) {
    config = std::move(**local);
  } else {
    auto platform = co_await fetch_platform_auth_entry(name, ctx);
    if (not platform) {
      diagnostic::error("unknown HTTP auth `{}`", name)
        .primary(loc)
        .note("configure the auth object under `tenzir.auth` or on the "
              "platform")
        .emit(ctx);
      co_return failure::promise();
    }
    config = std::move(*platform);
  }
  auto guard = co_await auth_cache().lock();
  auto it = guard->find(key);
  if (it == guard->end()) {
    it = guard
           ->emplace(key, Arc<AuthCacheEntry>{std::in_place, std::move(config)})
           .first;
  }
  co_return it->second;
}

auto strategy_of(located<record> const& auth_entry, diagnostic_handler& dh)
  -> failure_or<std::string> {
  auto const* strategy_data
    = auth_entry.inner.find("strategy") != auth_entry.inner.end()
        ? &auth_entry.inner.at("strategy")
        : nullptr;
  auto const* strategy
    = strategy_data ? try_as<std::string>(strategy_data) : nullptr;
  if (not strategy) {
    diagnostic::error("`auth.strategy` must be a `string`")
      .primary(auth_entry)
      .emit(dh);
    return failure::promise();
  }
  return *strategy;
}

auto make_authorization(std::string_view scheme, std::string value)
  -> AuthorizationConfig {
  auto auth = AuthorizationConfig{};
  auth.headers.emplace_back("Authorization",
                            fmt::format("{} {}", scheme, std::move(value)));
  return auth;
}

auto fetch_oauth_token(ResolvedOAuthAuth const& auth, OpCtx& ctx)
  -> Task<failure_or<std::pair<std::string, std::chrono::seconds>>> {
  auto body = record{
    {"client_id", auth.client_id},
    {"client_secret", auth.client_secret},
    {"grant_type", auth.grant_type},
  };
  if (not auth.scopes.empty()) {
    body["scope"] = detail::join(auth.scopes, " ");
  }
  if (auth.audience) {
    body["audience"] = *auth.audience;
  }
  auto body_string = curl::escape(body);
  auto headers = std::vector<http::Header>{
    {"Content-Type", "application/x-www-form-urlencoded"},
  };
  auto token_config = make_token_config(auth.token_url, {});
  auto pool = HttpPool::make(ctx.io_executor(), auth.token_url,
                             std::move(token_config));
  auto result = co_await pool->post(body_string, std::move(headers));
  if (result.is_err()) {
    diagnostic::error("failed to fetch HTTP auth token: {}",
                      std::move(result).unwrap_err())
      .primary(location::unknown)
      .emit(ctx);
    co_return failure::promise();
  }
  auto response = std::move(result).unwrap();
  if (not response.is_status_success()) {
    diagnostic::error("failed to fetch HTTP auth token: HTTP code `{}`",
                      response.status_code)
      .primary(location::unknown)
      .note("response body: {}", response.body)
      .emit(ctx);
    co_return failure::promise();
  }
  auto const json = from_json(response.body);
  if (not json.has_value()) {
    diagnostic::error("received no JSON when fetching HTTP auth token")
      .primary(location::unknown)
      .note("response body: {}", response.body)
      .emit(ctx);
    co_return failure::promise();
  }
  auto const* object = try_as<record>(json.value());
  if (not object) {
    diagnostic::error("HTTP auth token response body is not a JSON object")
      .primary(location::unknown)
      .note("response body: {}", response.body)
      .emit(ctx);
    co_return failure::promise();
  }
  auto parsed = parse_token_response(*object, location::unknown, ctx.dh());
  if (not parsed) {
    co_return failure::promise();
  }
  co_return *parsed;
}

template <class ParseFn, class ResolveFn, class BuildFn>
auto fetch_cached_simple_authorization(std::string_view name, OpCtx& ctx,
                                       ParseFn parse, ResolveFn resolve,
                                       BuildFn build)
  -> Task<failure_or<AuthorizationConfig>> {
  auto entry = co_await find_cached_auth_entry(name, location::unknown, ctx);
  if (not entry) {
    co_return failure::promise();
  }
  auto& cache_entry = *entry;
  auto state = co_await cache_entry->state.lock();
  if (state->authorization) {
    co_return *state->authorization;
  }
  auto config = cache_entry->config;
  state.unlock();
  auto opts = parse(std::move(config), ctx.dh());
  if (not opts) {
    co_return failure::promise();
  }
  auto resolved = co_await resolve(*opts, ctx);
  if (not resolved) {
    co_return failure::promise();
  }
  auto auth = build(*resolved);
  state = co_await cache_entry->state.lock();
  if (not state->authorization) {
    state->authorization = auth;
  }
  co_return *state->authorization;
}

auto fetch_oauth_authorization(std::string_view name, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>> {
  auto entry = co_await find_cached_auth_entry(name, location::unknown, ctx);
  if (not entry) {
    co_return failure::promise();
  }
  auto& cache_entry = *entry;
  auto state = co_await cache_entry->state.lock();
  if (state->oauth and not state->token.empty()
      and std::chrono::steady_clock::now() < state->refresh_at) {
    co_return make_authorization("Bearer", state->token);
  }
  auto config = cache_entry->config;
  auto cached_oauth = state->oauth;
  auto cached_token = state->token;
  auto cached_refresh_at = state->refresh_at;
  auto cached_expires_at = state->expires_at;
  state.unlock();

  if (not cached_oauth) {
    auto opts = parse_oauth_options(std::move(config), ctx.dh());
    if (not opts) {
      co_return failure::promise();
    }
    auto resolved = co_await resolve_oauth_auth(*opts, ctx);
    if (not resolved) {
      co_return failure::promise();
    }
    state = co_await cache_entry->state.lock();
    if (not state->oauth) {
      state->oauth = std::move(*resolved);
    }
    cached_oauth = state->oauth;
    cached_token = state->token;
    cached_refresh_at = state->refresh_at;
    cached_expires_at = state->expires_at;
    state.unlock();
  }

  if (not cached_oauth) {
    diagnostic::error("failed to initialize HTTP auth").primary(location::unknown)
      .emit(ctx);
    co_return failure::promise();
  }

  auto const now = std::chrono::steady_clock::now();
  if (cached_token.empty() or now >= cached_refresh_at) {
    auto parsed = co_await fetch_oauth_token(*cached_oauth, ctx);
    if (not parsed) {
      if (not cached_token.empty() and now < cached_expires_at) {
        co_return make_authorization("Bearer", cached_token);
      }
      co_return failure::promise();
    }
    auto const [token, expires_in] = *parsed;
    auto const now2 = std::chrono::steady_clock::now();
    state = co_await cache_entry->state.lock();
    if (not state->oauth) {
      state->oauth = *cached_oauth;
    }
    state->token = token;
    state->refresh_at = refresh_time(now2, expires_in);
    state->expires_at = now2 + expires_in;
    cached_token = state->token;
    cached_refresh_at = state->refresh_at;
    cached_expires_at = state->expires_at;
    state.unlock();
  }

  co_return make_authorization("Bearer", cached_token);
}

} // namespace

auto fetch_authorization(std::string_view name, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>> {
  auto entry = co_await find_cached_auth_entry(name, location::unknown, ctx);
  if (not entry) {
    co_return failure::promise();
  }
  auto& cache_entry = *entry;
  auto state = co_await cache_entry->state.lock();
  if (state->authorization) {
    co_return *state->authorization;
  }
  state.unlock();
  auto auth_entry = cache_entry->config;
  auto strategy = strategy_of(auth_entry, ctx.dh());
  if (not strategy) {
    co_return failure::promise();
  }
  if (*strategy == "oauth") {
    co_return co_await fetch_oauth_authorization(name, ctx);
  }
  if (*strategy == "basic") {
    co_return co_await fetch_cached_simple_authorization(
      name, ctx, parse_basic_options, resolve_basic_auth,
      [](ResolvedBasicAuth const& resolved) {
        auto raw = fmt::format("{}:{}", resolved.username, resolved.password);
        auto auth = AuthorizationConfig{};
        auth.headers.emplace_back("Authorization",
                                  fmt::format("Basic {}",
                                              detail::base64::encode(raw)));
        return auth;
      });
  }
  if (*strategy == "api-key") {
    co_return co_await fetch_cached_simple_authorization(
      name, ctx, parse_api_key_options, resolve_api_key_auth,
      [](ResolvedApiKeyAuth const& resolved) {
        auto auth = AuthorizationConfig{};
        auth.headers.emplace_back(resolved.header_name, resolved.api_key);
        return auth;
      });
  }
  if (*strategy == "bearer-static") {
    co_return co_await fetch_cached_simple_authorization(
      name, ctx, parse_bearer_static_options, resolve_bearer_static_auth,
      [](ResolvedBearerStaticAuth const& resolved) {
        return make_authorization("Bearer", resolved.token);
      });
  }
  diagnostic::error("unsupported auth strategy: `{}`", *strategy)
    .primary(cache_entry->config)
    .hint("supported strategies are `oauth`, `basic`, `api-key`, and "
          "`bearer-static`")
    .emit(ctx.dh());
  co_return failure::promise();
}

} // namespace tenzir
