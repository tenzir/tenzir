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
#include <iterator>

namespace tenzir {

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
  auto opts = aws_iam_options{};
  opts.loc = config.source;
  TRY(assign_secret(config, "region", opts.region, dh));
  TRY(assign_secret(config, "profile", opts.profile, dh));
  TRY(assign_secret(config, "assume_role", opts.role, dh));
  TRY(assign_secret(config, "session_name", opts.session_name, dh));
  TRY(assign_secret(config, "external_id", opts.external_id, dh));
  TRY(assign_secret(config, "access_key_id", opts.access_key_id, dh));
  TRY(assign_secret(config, "secret_access_key", opts.secret_access_key, dh));
  TRY(assign_secret(config, "session_token", opts.session_token, dh));
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
  if (web_identity) {
    resolved.web_identity = resolved_web_identity{};
    auto web_identity_requests
      = web_identity->make_secret_requests(*resolved.web_identity, dh);
    requests.insert(requests.end(),
                    std::make_move_iterator(web_identity_requests.begin()),
                    std::make_move_iterator(web_identity_requests.end()));
  }
  return requests;
}

namespace {

/// Checks whether the current auth mode requires an explicit AWS region.
auto check_region_requirement(const Option<aws_iam_options>& aws_iam,
                              const Option<located<std::string>>& aws_region,
                              AwsIamRegionRequirement requirement,
                              diagnostic_handler& dh) -> failure_or<void> {
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

auto resolve_aws_iam_auth(Option<aws_iam_options> aws_iam,
                          Option<located<std::string>> aws_region,
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

auto resolve_aws_iam_auth(Option<located<record>> aws_iam,
                          Option<located<std::string>> aws_region,
                          diagnostic_handler& dh,
                          AwsIamRegionRequirement requirement)
  -> failure_or<ResolvedAwsIamAuth> {
  auto parsed = Option<aws_iam_options>{};
  if (aws_iam) {
    TRY(parsed, aws_iam_options::from_record(std::move(*aws_iam), dh));
  }
  return resolve_aws_iam_auth(std::move(parsed), std::move(aws_region), dh,
                              requirement);
}

auto resolve_aws_iam_auth(Option<located<record>> aws_iam,
                          Option<located<std::string>> aws_region, OpCtx& ctx,
                          AwsIamRegionRequirement requirement)
  -> Task<Option<ResolvedAwsIamAuth>> {
  auto auth = resolve_aws_iam_auth(std::move(aws_iam), std::move(aws_region),
                                   ctx.dh(), requirement);
  if (not auth) {
    co_return None{};
  }
  if (auth->options and auth->credentials) {
    auto requests
      = auth->options->make_secret_requests(*auth->credentials, ctx.dh());
    if (not co_await ctx.resolve_secrets(std::move(requests))) {
      co_return None{};
    }
  }
  co_return std::move(*auth);
}

} // namespace tenzir
