//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/aws_iam.hpp"

#include "tenzir/async.hpp"
#include "tenzir/diagnostics.hpp"

#include <algorithm>
#include <array>

namespace tenzir {

namespace {

/// Helper to check for CRLF injection characters in strings.
auto contains_crlf(std::string_view s) -> bool {
  return s.find('\r') != std::string_view::npos
         or s.find('\n') != std::string_view::npos;
}

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

/// Helper to parse headers from a record.
auto parse_headers(const located<record>& config,
                   std::optional<std::vector<std::pair<std::string, secret>>>& x,
                   diagnostic_handler& dh) -> failure_or<void> {
  auto it = config.inner.find("headers");
  if (it == config.inner.end()) {
    return {};
  }
  auto* r = try_as<record>(it->second.get_data());
  if (not r) {
    diagnostic::error("`headers` must be a record").primary(config).emit(dh);
    return failure::promise();
  }
  x = std::vector<std::pair<std::string, secret>>{};
  for (auto& [key, value] : *r) {
    // Validate header name for CRLF injection.
    if (contains_crlf(key)) {
      diagnostic::error("header name '{}' contains invalid characters "
                        "(newline/carriage return)",
                        key)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    if (auto* s = try_as<secret>(value.get_data())) {
      x->emplace_back(key, std::move(*s));
    } else if (auto* str = try_as<std::string>(value.get_data())) {
      if (str->empty()) {
        diagnostic::error("header '{}' value must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      // Validate header value for CRLF injection.
      if (contains_crlf(*str)) {
        diagnostic::error("header '{}' value contains invalid characters "
                          "(newline/carriage return)",
                          key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x->emplace_back(key, secret::make_literal(std::move(*str)));
    } else {
      diagnostic::error("header '{}' value must be a `string` or `secret`", key)
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

} // namespace

auto token_endpoint_options::from_record(located<record> config,
                                         diagnostic_handler& dh)
  -> failure_or<token_endpoint_options> {
  constexpr auto known = std::array{"url", "headers", "path"};
  const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in token_endpoint config",
                      (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = token_endpoint_options{};
  opts.loc = config.source;
  TRY(assign_secret(config, "url", opts.url, dh));
  TRY(parse_headers(config, opts.headers, dh));
  // Parse path: string, or null for plain text response.
  if (auto it = config.inner.find("path"); it != config.inner.end()) {
    if (is<caf::none_t>(it->second.get_data())) {
      // Explicit null means plain text response (no JSON parsing).
      opts.path_is_null = true;
    } else if (auto* str = try_as<std::string>(it->second.get_data())) {
      if (str->empty()) {
        diagnostic::error("`path` must not be empty").primary(config).emit(dh);
        return failure::promise();
      }
      opts.path = std::move(*str);
    } else {
      diagnostic::error("`path` must be a `string` or `null`")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  // Validate: url is required.
  if (not opts.url) {
    diagnostic::error("`token_endpoint` requires `url`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  return opts;
}

auto web_identity_options::from_record(located<record> config,
                                       diagnostic_handler& dh)
  -> failure_or<web_identity_options> {
  constexpr auto known = std::array{"token_endpoint", "token_file", "token"};
  const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in web_identity config",
                      (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  auto opts = web_identity_options{};
  opts.loc = config.source;
  // Parse token_endpoint as a nested record.
  if (auto it = config.inner.find("token_endpoint"); it != config.inner.end()) {
    if (auto* r = try_as<record>(it->second.get_data())) {
      auto endpoint_config = located{std::move(*r), config.source};
      TRY(opts.token_endpoint,
          token_endpoint_options::from_record(std::move(endpoint_config), dh));
    } else {
      diagnostic::error("`token_endpoint` must be a record")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  TRY(assign_secret(config, "token_file", opts.token_file, dh));
  TRY(assign_secret(config, "token", opts.token, dh));
  // Validate: exactly one of token_endpoint, token_file, token must be set.
  const auto token_source_count = (opts.token_endpoint.has_value() ? 1 : 0)
                                  + (opts.token_file.has_value() ? 1 : 0)
                                  + (opts.token.has_value() ? 1 : 0);
  if (token_source_count == 0) {
    diagnostic::error(
      "`web_identity` requires one of: `token_endpoint`, `token_file`, `token`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  if (token_source_count > 1) {
    diagnostic::error(
      "`token_endpoint`, `token_file`, and `token` are mutually exclusive")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  return opts;
}

auto aws_iam_options::from_record(located<record> config,
                                  diagnostic_handler& dh)
  -> failure_or<aws_iam_options> {
  constexpr auto known = std::array{
    "region",       "profile",       "assume_role",       "session_name",
    "external_id",  "access_key_id", "secret_access_key", "session_token",
    "web_identity",
  };
  const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in config", (*unknown).first)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  const auto assign_secret
    = [&](std::string_view key, std::optional<secret>& x) -> failure_or<void> {
    if (auto it = config.inner.find(key); it != config.inner.end()) {
      if (auto* s = try_as<secret>(it->second.get_data())) {
        x = std::move(*s);
      } else if (auto* str = try_as<std::string>(it->second.get_data())) {
        // Allow plain strings as well, convert to literal secret
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
  };
  auto opts = aws_iam_options{};
  opts.loc = config.source;
  TRY(assign_secret("region", opts.region));
  TRY(assign_secret("profile", opts.profile));
  TRY(assign_secret("assume_role", opts.role));
  TRY(assign_secret("session_name", opts.session_name));
  TRY(assign_secret("external_id", opts.external_id));
  TRY(assign_secret("access_key_id", opts.access_key_id));
  TRY(assign_secret("secret_access_key", opts.secret_access_key));
  TRY(assign_secret("session_token", opts.session_token));
  // Parse web_identity as a nested record.
  if (auto it = config.inner.find("web_identity"); it != config.inner.end()) {
    if (auto* r = try_as<record>(it->second.get_data())) {
      auto web_identity_config = located{std::move(*r), config.source};
      TRY(opts.web_identity, web_identity_options::from_record(
                               std::move(web_identity_config), dh));
    } else {
      diagnostic::error("`web_identity` must be a record")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  // Validate that access_key_id and secret_access_key are specified together
  if (opts.access_key_id.has_value() xor opts.secret_access_key.has_value()) {
    diagnostic::error(
      "`access_key_id` and `secret_access_key` must be specified together")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  // Validate that session_token requires access_key_id
  if (opts.session_token and not opts.access_key_id) {
    diagnostic::error("`session_token` specified without `access_key_id`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  // Validate that profile is not used with explicit credentials
  if (opts.profile and opts.access_key_id) {
    diagnostic::error("`profile` cannot be used with explicit credentials")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  // Validate that external_id requires assume_role
  if (opts.external_id and not opts.role) {
    diagnostic::error("`external_id` specified without `assume_role`")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  // Validate web_identity constraints.
  if (opts.web_identity) {
    // web_identity requires assume_role.
    if (not opts.role) {
      diagnostic::error("`web_identity` requires `assume_role` to be specified")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    // web_identity cannot be used with explicit credentials.
    if (opts.access_key_id) {
      diagnostic::error(
        "`web_identity` cannot be used with explicit credentials")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
    // web_identity cannot be used with profile.
    if (opts.profile) {
      diagnostic::error("`web_identity` cannot be used with `profile`")
        .primary(config)
        .emit(dh);
      return failure::promise();
    }
  }
  return opts;
}

auto aws_iam_options::make_secret_requests(resolved_aws_credentials& resolved,
                                           diagnostic_handler& dh) const
  -> std::vector<secret_request> {
  auto requests = std::vector<secret_request>{};
  // Resolve all optional fields that may be secrets
  if (region) {
    requests.emplace_back(
      make_secret_request("region", *region, loc, resolved.region, dh));
  }
  if (profile) {
    requests.emplace_back(
      make_secret_request("profile", *profile, loc, resolved.profile, dh));
  }
  if (session_name) {
    requests.emplace_back(make_secret_request("session_name", *session_name,
                                              loc, resolved.session_name, dh));
  }
  if (access_key_id) {
    requests.emplace_back(make_secret_request("access_key_id", *access_key_id,
                                              loc, resolved.access_key_id, dh));
    requests.emplace_back(make_secret_request("secret_access_key",
                                              *secret_access_key, loc,
                                              resolved.secret_access_key, dh));
    if (session_token) {
      requests.emplace_back(make_secret_request(
        "session_token", *session_token, loc, resolved.session_token, dh));
    }
  }
  if (role) {
    requests.emplace_back(
      make_secret_request("assume_role", *role, loc, resolved.role, dh));
    if (external_id) {
      requests.emplace_back(make_secret_request("external_id", *external_id,
                                                loc, resolved.external_id, dh));
    }
  }
  // Resolve web_identity secrets.
  if (web_identity) {
    resolved.web_identity = resolved_web_identity{};
    auto& wi = *resolved.web_identity;
    if (web_identity->token_endpoint) {
      const auto& te = *web_identity->token_endpoint;
      wi.token_endpoint = resolved_token_endpoint{};
      auto& rte = *wi.token_endpoint;
      requests.emplace_back(make_secret_request("token_endpoint.url", *te.url,
                                                te.loc, rte.url, dh));
      // Resolve header secrets.
      if (te.headers) {
        // Pre-allocate space for resolved headers.
        rte.headers.reserve(te.headers->size());
        for (const auto& [key, value] : *te.headers) {
          // Add a placeholder that will be filled by secret resolution.
          rte.headers.emplace_back(key, std::string{});
          requests.emplace_back(
            make_secret_request(fmt::format("header '{}'", key), value, te.loc,
                                rte.headers.back().second, dh));
        }
      }
      // Handle path: explicit value, explicit null, or default.
      if (te.path_is_null) {
        // Explicit null: plain text response (no JSON parsing).
        rte.path = std::nullopt;
      } else if (te.path) {
        // Explicit JSON path.
        rte.path = *te.path;
      } else {
        // Default: ".access_token" for JSON response.
        rte.path = ".access_token";
      }
    }
    if (web_identity->token_file) {
      requests.emplace_back(
        make_secret_request("token_file", *web_identity->token_file,
                            web_identity->loc, wi.token_file, dh));
    }
    if (web_identity->token) {
      requests.emplace_back(make_secret_request(
        "token", *web_identity->token, web_identity->loc, wi.token, dh));
    }
  }
  return requests;
}

namespace {

/// Checks whether the current auth mode requires an explicit AWS region.
auto check_region_requirement(
  const std::optional<aws_iam_options>& aws_iam,
  const std::optional<located<std::string>>& aws_region,
  AwsIamRegionRequirement requirement, diagnostic_handler& dh)
  -> failure_or<void> {
  if (not aws_iam) {
    return {};
  }
  if (requirement != AwsIamRegionRequirement::required_with_iam) {
    return {};
  }
  if (aws_region or aws_iam->region) {
    return {};
  }
  diagnostic::error("`aws_region` is required for AWS IAM authentication")
    .primary(aws_iam->loc)
    .emit(dh);
  return failure::promise();
}

} // namespace

auto resolve_aws_iam_auth(std::optional<aws_iam_options> aws_iam,
                          std::optional<located<std::string>> aws_region,
                          diagnostic_handler& dh,
                          AwsIamRegionRequirement requirement)
  -> failure_or<ResolvedAwsIamAuth> {
  TRY(check_region_requirement(aws_iam, aws_region, requirement, dh));
  auto result = ResolvedAwsIamAuth{};
  result.options = std::move(aws_iam);
  if (result.options) {
    result.credentials.emplace();
  }
  if (aws_region) {
    if (not result.credentials) {
      result.credentials.emplace();
    }
    result.credentials->region = aws_region->inner;
  }
  return result;
}

auto resolve_aws_iam_auth(std::optional<located<record>> aws_iam,
                          std::optional<located<std::string>> aws_region,
                          diagnostic_handler& dh,
                          AwsIamRegionRequirement requirement)
  -> failure_or<ResolvedAwsIamAuth> {
  auto parsed = std::optional<aws_iam_options>{};
  if (aws_iam) {
    TRY(parsed, aws_iam_options::from_record(std::move(*aws_iam), dh));
  }
  return resolve_aws_iam_auth(std::move(parsed), std::move(aws_region), dh,
                              requirement);
}

auto resolve_aws_iam_auth(std::optional<located<record>> aws_iam,
                          std::optional<located<std::string>> aws_region,
                          OpCtx& ctx, AwsIamRegionRequirement requirement)
  -> Task<std::optional<ResolvedAwsIamAuth>> {
  auto auth = resolve_aws_iam_auth(std::move(aws_iam), std::move(aws_region),
                                   ctx.dh(), requirement);
  if (not auth) {
    co_return std::nullopt;
  }
  if (auth->options and auth->credentials) {
    auto requests
      = auth->options->make_secret_requests(*auth->credentials, ctx.dh());
    if (not co_await ctx.resolve_secrets(std::move(requests))) {
      co_return std::nullopt;
    }
  }
  co_return std::move(*auth);
}

} // namespace tenzir
