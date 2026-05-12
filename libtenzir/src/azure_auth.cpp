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
#include "tenzir/http_pool.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"

#include <folly/io/async/EventBase.h>

#include <boost/url/parse.hpp>

#include <algorithm>
#include <array>
#include <chrono>

namespace tenzir {

namespace {

using namespace std::chrono_literals;

constexpr auto default_authority = "https://login.microsoftonline.com";

/// Helper to assign a secret from a record field.
auto assign_secret(const located<record>& config, std::string_view key,
                   std::optional<secret>& x, diagnostic_handler& dh)
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

auto token_endpoint(resolved_azure_auth const& auth) -> std::string {
  return fmt::format("{}/{}/oauth2/v2.0/token", auth.authority, auth.tenant_id);
}

auto parse_expires_in(record const& response) -> std::chrono::seconds {
  auto it = response.find("expires_in");
  if (it == response.end()) {
    return 50min;
  }
  auto seconds = uint64_t{};
  if (auto* value = try_as<uint64_t>(it->second)) {
    seconds = *value;
  } else if (auto* value = try_as<int64_t>(it->second); value and *value > 0) {
    seconds = static_cast<uint64_t>(*value);
  } else if (auto* value = try_as<double>(it->second); value and *value > 0) {
    seconds = static_cast<uint64_t>(*value);
  } else {
    return 50min;
  }
  return std::chrono::seconds{seconds};
}

auto refresh_time(std::chrono::seconds expires_in)
  -> std::chrono::steady_clock::time_point {
  auto safety_margin = 60s;
  if (expires_in <= 2 * safety_margin) {
    safety_margin = expires_in / 2;
  }
  return std::chrono::steady_clock::now() + expires_in - safety_margin;
}

} // namespace

auto azure_auth_options::from_record(located<record> config,
                                     diagnostic_handler& dh)
  -> failure_or<azure_auth_options> {
  constexpr auto known = std::array{
    "tenant_id", "client_id", "client_secret", "scope", "authority",
  };
  const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in `auth`", (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = azure_auth_options{};
  opts.loc = config.source;
  TRY(assign_secret(config, "tenant_id", opts.tenant_id, dh));
  TRY(assign_secret(config, "client_id", opts.client_id, dh));
  TRY(assign_secret(config, "client_secret", opts.client_secret, dh));
  TRY(assign_secret(config, "scope", opts.scope, dh));
  TRY(assign_secret(config, "authority", opts.authority, dh));
  if (not opts.tenant_id) {
    diagnostic::error("`auth` requires `tenant_id`").primary(config).emit(dh);
    return failure::promise();
  }
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
  return opts;
}

auto azure_auth_options::make_secret_requests(resolved_azure_auth& resolved,
                                              std::string default_scope,
                                              diagnostic_handler& dh) const
  -> std::vector<secret_request> {
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(make_secret_request("auth.tenant_id", *tenant_id, loc,
                                            resolved.tenant_id, dh));
  requests.emplace_back(make_secret_request("auth.client_id", *client_id, loc,
                                            resolved.client_id, dh));
  requests.emplace_back(make_secret_request(
    "auth.client_secret", *client_secret, loc, resolved.client_secret, dh));
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

auto resolve_azure_auth(azure_auth_options options, std::string default_scope,
                        OpCtx& ctx)
  -> Task<std::optional<resolved_azure_auth>> {
  auto resolved = resolved_azure_auth{};
  auto requests = options.make_secret_requests(
    resolved, std::move(default_scope), ctx.dh());
  if (auto result = co_await ctx.resolve_secrets(std::move(requests));
      result.is_error()) {
    co_return std::nullopt;
  }
  auto& dh = ctx.dh();
  if (not check_resolved("tenant_id", resolved.tenant_id, options.loc, dh)
      or not check_resolved("client_id", resolved.client_id, options.loc, dh)
      or not check_resolved("client_secret", resolved.client_secret,
                            options.loc, dh)
      or not check_resolved("scope", resolved.scope, options.loc, dh)
      or not check_resolved("authority", resolved.authority, options.loc, dh)) {
    co_return std::nullopt;
  }
  resolved.authority = normalize_authority(std::move(resolved.authority));
  if (not validate_authority(resolved.authority, options.loc, dh)) {
    co_return std::nullopt;
  }
  co_return resolved;
}

AzureTokenProvider::AzureTokenProvider(resolved_azure_auth auth, location loc)
  : auth_{std::move(auth)}, loc_{loc} {
}

auto AzureTokenProvider::authorize(std::map<std::string, std::string>& headers,
                                   OpCtx& ctx) -> Task<failure_or<void>> {
  CO_TRY(auto token, co_await this->token(ctx));
  headers["Authorization"] = fmt::format("Bearer {}", token);
  co_return {};
}

auto AzureTokenProvider::token(OpCtx& ctx) -> Task<failure_or<std::string>> {
  if (not token_.empty() and std::chrono::steady_clock::now() < refresh_at_) {
    co_return token_;
  }
  CO_TRY(co_await refresh(ctx));
  co_return token_;
}

auto AzureTokenProvider::refresh(OpCtx& ctx) -> Task<failure_or<void>> {
  auto& dh = ctx.dh();
  const auto url = token_endpoint(auth_);
  const auto body = curl::escape(record{
    {"client_id", auth_.client_id},
    {"client_secret", auth_.client_secret},
    {"grant_type", "client_credentials"},
    {"scope", auth_.scope},
  });
  auto headers = std::map<std::string, std::string>{
    {"Content-Type", "application/x-www-form-urlencoded"},
    {"Content-Length", fmt::to_string(body.size())},
  };
  auto result = co_await http_post(ctx.io_executor()->getEventBase(), url, body,
                                   std::move(headers));
  if (result.is_err()) {
    diagnostic::error("failed to fetch Azure access token: {}",
                      std::move(result).unwrap_err())
      .primary(loc_)
      .emit(dh);
    co_return failure::promise();
  }
  auto response = std::move(result).unwrap();
  auto fail = [&](std::string message) -> failure_or<void> {
    diagnostic_builder{severity::error, std::move(message)}
      .primary(loc_)
      .note("response body: {}", response.body)
      .emit(dh);
    return failure::promise();
  };
  if (not response.is_status_success()) {
    co_return fail(
      fmt::format("failed to fetch Azure access token: HTTP code `{}`",
                  response.status_code));
  }
  const auto json = from_json(response.body);
  if (not json.has_value()) {
    co_return fail("received no JSON when fetching Azure access token");
  }
  const auto* object = try_as<record>(json.value());
  if (not object) {
    co_return fail("Azure token response body is not a JSON object");
  }
  const auto it = object->find("access_token");
  if (it == object->end()) {
    co_return fail("Azure token response does not contain `access_token`");
  }
  const auto* token = try_as<std::string>(it->second);
  if (not token) {
    co_return fail(
      fmt::format("expected Azure `access_token` to be `string`, got `{}`",
                  type::infer(it->second).value_or(type{}).kind()));
  }
  token_ = *token;
  refresh_at_ = refresh_time(parse_expires_in(*object));
  co_return {};
}

} // namespace tenzir
