//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http_auth.hpp"

#include "tenzir/arc.hpp"
#include "tenzir/async/fetch_node.hpp"
#include "tenzir/async/mail.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ecc.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/location.hpp"
#include "tenzir/option.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/secret_store.hpp"
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

auto read_string(located<record> const& config, std::string_view key,
                 diagnostic_handler& dh) -> failure_or<Option<std::string>> {
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      return *str;
    }
    diagnostic::error("'{}' must be a `string`", key).primary(config).emit(dh);
    return failure::promise();
  }
  return None{};
}

auto read_secret(located<record> const& config, std::string_view key,
                 diagnostic_handler& dh) -> failure_or<Option<secret>> {
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* s = try_as<secret>(it->second.get_data())) {
      return *s;
    }
    if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      return secret::make_literal(*str);
    }
    diagnostic::error("'{}' must be a `string` or `secret`", key)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  return None{};
}

auto read_secret_list(located<record> const& config, std::string_view key,
                      diagnostic_handler& dh)
  -> failure_or<std::vector<secret>> {
  auto result = std::vector<secret>{};
  if (auto it = config.inner.find(key); it != config.inner.end()) {
    if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      result.push_back(secret::make_literal(*str));
      return result;
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
        result.push_back(*s);
      } else if (auto* str = try_as<std::string>(&value)) {
        if (str->empty()) {
          diagnostic::error("scope values must not be empty")
            .primary(config)
            .emit(dh);
          return failure::promise();
        }
        result.push_back(secret::make_literal(*str));
      } else {
        diagnostic::error("scope values must be `string` or `secret`")
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
    }
  }
  return result;
}

auto require_secret(located<record> const& config, std::string_view key,
                    diagnostic_handler& dh) -> failure_or<secret> {
  TRY(auto value, read_secret(config, key, dh));
  if (not value) {
    diagnostic::error("`auth` requires `{}`", key).primary(config).emit(dh);
    return failure::promise();
  }
  return *value;
}

template <class Known>
auto validate_known_keys(located<record> const& config, Known const& known,
                         diagnostic_handler& dh) -> failure_or<void> {
  auto const unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in `auth`", (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
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

auto parse_oauth_options(located<record> config, diagnostic_handler& dh)
  -> failure_or<OAuthAuthOptions> {
  constexpr auto known = std::array{
    "name",      "strategy", "client_id",  "client_secret",
    "token_url", "scopes",   "grant_type", "audience",
  };
  TRY(validate_known_keys(config, known, dh));
  auto opts = OAuthAuthOptions{};
  opts.loc = config.source;
  TRY(auto client_id, require_secret(config, "client_id", dh));
  TRY(auto client_secret, require_secret(config, "client_secret", dh));
  TRY(auto token_url, require_secret(config, "token_url", dh));
  TRY(auto scopes, read_secret_list(config, "scopes", dh));
  TRY(auto grant_type, read_string(config, "grant_type", dh));
  TRY(auto audience, read_string(config, "audience", dh));
  opts.client_id = std::move(client_id);
  opts.client_secret = std::move(client_secret);
  opts.token_url = std::move(token_url);
  opts.scopes = std::move(scopes);
  if (grant_type) {
    opts.grant_type = std::move(*grant_type);
  }
  opts.audience = std::move(audience);
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

auto resolve_secret_string(std::string_view key, secret value, location loc,
                           OpCtx& ctx) -> Task<failure_or<std::string>> {
  auto result = std::string{};
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(make_secret_request(
    fmt::format("auth.{}", key), std::move(value), loc, result, ctx.dh()));
  if (auto resolved = co_await ctx.resolve_secrets(std::move(requests));
      resolved.is_error()) {
    co_return failure::promise();
  }
  if (not check_resolved(key, result, loc, ctx.dh())) {
    co_return failure::promise();
  }
  co_return result;
}

auto resolve_oauth_auth(OAuthAuthOptions const& options, OpCtx& ctx)
  -> Task<failure_or<ResolvedOAuthAuth>> {
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
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return failure::promise();
  }
  resolved.grant_type = options.grant_type;
  if (options.audience) {
    resolved.audience = *options.audience;
  }
  if (not check_resolved("client_id", resolved.client_id, options.loc,
                         ctx.dh())) {
    co_return failure::promise();
  }
  if (not check_resolved("client_secret", resolved.client_secret, options.loc,
                         ctx.dh())) {
    co_return failure::promise();
  }
  if (not check_resolved("token_url", resolved.token_url, options.loc,
                         ctx.dh())) {
    co_return failure::promise();
  }
  if (not check_resolved("grant_type", resolved.grant_type, options.loc,
                         ctx.dh())) {
    co_return failure::promise();
  }
  TRY(validate_url(resolved.token_url, options.loc, ctx.dh()));
  for (auto const& scope : resolved.scopes) {
    if (scope.empty()) {
      diagnostic::error("`auth.scopes` must not contain empty values")
        .primary(options.loc)
        .emit(ctx.dh());
      co_return failure::promise();
    }
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

using AuthCacheKey = std::string;

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
    co_return None{};
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
  if (not found) {
    co_return None{};
  }
  co_return found;
}

auto fetch_platform_auth_entry(std::string_view name, location loc, OpCtx& ctx)
  -> Task<failure_or<located<record>>> {
  auto node = co_await fetch_node(ctx.actor_system(), ctx.dh());
  if (not node or not *node) {
    co_return failure::promise();
  }
  auto key_pair = ecc::generate_keypair();
  TENZIR_ASSERT(key_pair);
  auto result = co_await async_mail(atom::resolve_v, atom::authentication_v,
                                    std::string{name}, key_pair->public_key)
                  .request(*node);
  if (not result) {
    diagnostic::error(result.error()).primary(loc).emit(ctx);
    co_return failure::promise();
  }
  auto config = record{};
  auto success = match(
    *result,
    [&](platform_authentication const& auth) -> bool {
      config = auth.public_config;
      for (auto it = config.begin(); it != config.end();) {
        if (is<caf::none_t>(it->second)) {
          it = config.erase(it);
        } else {
          ++it;
        }
      }
      config["name"] = std::string{name};
      config["strategy"] = auth.strategy;
      for (auto const& [field, encrypted] : auth.encrypted_secret_fields) {
        auto decrypted = ecc::decrypt_string(encrypted, *key_pair);
        if (not decrypted) {
          diagnostic::error("failed to decrypt HTTP auth field `{}`: {}", field,
                            decrypted.error())
            .primary(loc)
            .emit(ctx);
          return false;
        }
        config[field] = secret::make_literal(std::move(*decrypted));
      }
      return true;
    },
    [&](secret_resolution_error const& e) -> bool {
      diagnostic::error("unknown HTTP auth `{}`", name)
        .primary(loc)
        .note("{}", e.message)
        .emit(ctx);
      return false;
    });
  if (not success) {
    co_return failure::promise();
  }
  co_return located{std::move(config), location::unknown};
}

auto find_auth_entry(std::string_view name, location loc, OpCtx& ctx)
  -> Task<failure_or<located<record>>> {
  auto local = co_await find_local_auth_entry(name, loc, ctx);
  if (not local) {
    co_return failure::promise();
  }
  if (*local) {
    co_return **local;
  }
  co_return co_await fetch_platform_auth_entry(name, loc, ctx);
}

auto find_cached_auth_entry(std::string_view name, location loc, OpCtx& ctx,
                            bool needs_refresh)
  -> Task<failure_or<Arc<AuthCacheEntry>>> {
  auto key = AuthCacheKey{name};
  if (not needs_refresh) {
    auto guard = co_await auth_cache().lock();
    if (auto it = guard->find(key); it != guard->end()) {
      co_return it->second;
    }
  }
  auto config = co_await find_auth_entry(name, loc, ctx);
  if (not config) {
    co_return failure::promise();
  }
  auto guard = co_await auth_cache().lock();
  auto it = guard->find(key);
  if (it == guard->end() or needs_refresh) {
    it = guard
           ->insert_or_assign(std::move(key),
                              Arc<AuthCacheEntry>{std::in_place,
                                                  std::move(*config)})
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

auto fetch_basic_authorization(located<record> config, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>> {
  constexpr auto known = std::array{"name", "strategy", "username", "password"};
  TRY(validate_known_keys(config, known, ctx.dh()));
  TRY(auto username_secret, require_secret(config, "username", ctx.dh()));
  TRY(auto password_secret, require_secret(config, "password", ctx.dh()));
  auto username = co_await resolve_secret_string(
    "username", std::move(username_secret), config.source, ctx);
  if (not username) {
    co_return failure::promise();
  }
  auto password = co_await resolve_secret_string(
    "password", std::move(password_secret), config.source, ctx);
  if (not password) {
    co_return failure::promise();
  }
  auto raw = fmt::format("{}:{}", *username, *password);
  co_return make_authorization("Basic", detail::base64::encode(raw));
}

auto fetch_api_key_authorization(located<record> config, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>> {
  constexpr auto known
    = std::array{"name", "strategy", "api_key", "header_name"};
  TRY(validate_known_keys(config, known, ctx.dh()));
  TRY(auto api_key_secret, require_secret(config, "api_key", ctx.dh()));
  TRY(auto header_name, read_string(config, "header_name", ctx.dh()));
  auto api_key = co_await resolve_secret_string(
    "api_key", std::move(api_key_secret), config.source, ctx);
  if (not api_key) {
    co_return failure::promise();
  }
  auto header = std::string{default_api_key_header_name};
  if (header_name) {
    header = std::move(*header_name);
  }
  auto auth = AuthorizationConfig{};
  auth.headers.emplace_back(std::move(header), *api_key);
  co_return auth;
}

auto fetch_bearer_static_authorization(located<record> config, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>> {
  constexpr auto known = std::array{"name", "strategy", "token"};
  TRY(validate_known_keys(config, known, ctx.dh()));
  TRY(auto token_secret, require_secret(config, "token", ctx.dh()));
  auto token = co_await resolve_secret_string("token", std::move(token_secret),
                                              config.source, ctx);
  if (not token) {
    co_return failure::promise();
  }
  co_return make_authorization("Bearer", *token);
}

auto cache_authorization(AuthCacheEntry& entry, AuthorizationConfig auth)
  -> Task<AuthorizationConfig> {
  auto state = co_await entry.state.lock();
  if (not state->authorization) {
    state->authorization = std::move(auth);
  }
  co_return *state->authorization;
}

auto cache_oauth_authorization(AuthCacheEntry& entry,
                               ResolvedOAuthAuth const& oauth,
                               std::string token,
                               std::chrono::steady_clock::time_point refresh_at,
                               std::chrono::steady_clock::time_point expires_at)
  -> Task<void> {
  auto state = co_await entry.state.lock();
  if (not state->oauth) {
    state->oauth = oauth;
  }
  state->token = std::move(token);
  state->refresh_at = refresh_at;
  state->expires_at = expires_at;
}

auto load_cached_auth_state(AuthCacheEntry& entry) -> Task<AuthCacheState> {
  auto state = co_await entry.state.lock();
  co_return *state;
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

auto fetch_oauth_authorization(AuthCacheEntry& cache_entry, OpCtx& ctx)
  -> Task<failure_or<AuthorizationConfig>> {
  auto cached = co_await load_cached_auth_state(cache_entry);
  if (cached.oauth and not cached.token.empty()
      and std::chrono::steady_clock::now() < cached.refresh_at) {
    co_return make_authorization("Bearer", cached.token);
  }
  auto config = cache_entry.config;

  if (not cached.oauth) {
    auto opts = parse_oauth_options(std::move(config), ctx.dh());
    if (not opts) {
      co_return failure::promise();
    }
    auto resolved = co_await resolve_oauth_auth(*opts, ctx);
    if (not resolved) {
      co_return failure::promise();
    }
    auto state = co_await cache_entry.state.lock();
    if (not state->oauth) {
      state->oauth = std::move(*resolved);
    }
    cached = *state;
  }

  if (not cached.oauth) {
    diagnostic::error("failed to initialize HTTP auth")
      .primary(location::unknown)
      .emit(ctx);
    co_return failure::promise();
  }

  auto const now = std::chrono::steady_clock::now();
  if (cached.token.empty() or now >= cached.refresh_at) {
    auto parsed = co_await fetch_oauth_token(*cached.oauth, ctx);
    if (not parsed) {
      if (not cached.token.empty() and now < cached.expires_at) {
        co_return make_authorization("Bearer", cached.token);
      }
      co_return failure::promise();
    }
    auto const [token, expires_in] = *parsed;
    auto const now2 = std::chrono::steady_clock::now();
    auto refresh_at = refresh_time(now2, expires_in);
    auto expires_at = now2 + expires_in;
    co_await cache_oauth_authorization(
      cache_entry, *cached.oauth, std::move(token), refresh_at, expires_at);
    cached = co_await load_cached_auth_state(cache_entry);
  }

  co_return make_authorization("Bearer", cached.token);
}

} // namespace

auto fetch_authorization(std::string_view name, OpCtx& ctx, bool needs_refresh)
  -> Task<failure_or<AuthorizationConfig>> {
  auto entry = co_await find_cached_auth_entry(name, location::unknown, ctx,
                                               needs_refresh);
  if (not entry) {
    co_return failure::promise();
  }
  auto& cache_entry = *entry;
  auto cached = co_await load_cached_auth_state(cache_entry);
  if (cached.authorization) {
    co_return *cached.authorization;
  }
  auto strategy = strategy_of(cache_entry->config, ctx.dh());
  if (not strategy) {
    co_return failure::promise();
  }
  if (*strategy == "oauth" or *strategy == "oauth-client-credentials") {
    co_return co_await fetch_oauth_authorization(*cache_entry, ctx);
  }
  if (*strategy == "basic") {
    auto auth = co_await fetch_basic_authorization(cache_entry->config, ctx);
    if (not auth) {
      co_return failure::promise();
    }
    co_return co_await cache_authorization(*cache_entry, std::move(*auth));
  }
  if (*strategy == "api-key") {
    auto auth = co_await fetch_api_key_authorization(cache_entry->config, ctx);
    if (not auth) {
      co_return failure::promise();
    }
    co_return co_await cache_authorization(*cache_entry, std::move(*auth));
  }
  if (*strategy == "bearer-static") {
    auto auth
      = co_await fetch_bearer_static_authorization(cache_entry->config, ctx);
    if (not auth) {
      co_return failure::promise();
    }
    co_return co_await cache_authorization(*cache_entry, std::move(*auth));
  }
  diagnostic::error("unsupported auth strategy: `{}`", *strategy)
    .primary(cache_entry->config)
    .hint("supported strategies are `oauth-client-credentials`, `oauth`, "
          "`basic`, `api-key`, and `bearer-static`")
    .emit(ctx.dh());
  co_return failure::promise();
}

} // namespace tenzir
