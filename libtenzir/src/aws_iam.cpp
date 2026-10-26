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

auto aws_iam_options::from_record(located<nova::Record> config,
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
    if (auto* r = try_as<nova::Record>(&it->second)) {
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
  // Point at the opening brace, as the record often spans many lines.
  diagnostic::error("`aws_region` is required for AWS IAM authentication")
    .primary(aws_iam->loc.subloc(0, 1))
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
  if (not aws_iam and not aws_region) {
    return result;
  }
  auto& credentials = result.credentials.emplace();
  if (aws_region) {
    credentials.region = aws_region->inner;
  }
  if (aws_iam) {
    auto const& opts = *aws_iam;
    auto const loc = opts.loc;
    if (opts.region) {
      TRY(credentials.region, reveal_string(opts.region, "region", loc, dh));
    }
    TRY(credentials.profile, reveal_string(opts.profile, "profile", loc, dh));
    TRY(credentials.session_name,
        reveal_string(opts.session_name, "session_name", loc, dh));
    if (opts.access_key_id) {
      TRY(credentials.access_key_id,
          reveal_string(opts.access_key_id, "access_key_id", loc, dh));
      TRY(credentials.secret_access_key,
          reveal_string(opts.secret_access_key, "secret_access_key", loc, dh));
      TRY(credentials.session_token,
          reveal_string(opts.session_token, "session_token", loc, dh));
    }
    if (opts.role) {
      TRY(credentials.role, reveal_string(opts.role, "assume_role", loc, dh));
      TRY(credentials.external_id,
          reveal_string(opts.external_id, "external_id", loc, dh));
    }
    if (opts.web_identity) {
      TRY(credentials.web_identity, opts.web_identity->resolve(dh));
    }
  }
  result.options = std::move(aws_iam);
  return result;
}

auto resolve_aws_iam_auth(Option<StartupArgument<nova::Record>> const& aws_iam,
                          Option<located<std::string>> aws_region, OpCtx& ctx,
                          AwsIamRegionRequirement requirement)
  -> Task<Option<ResolvedAwsIamAuth>> {
  auto options = Option<aws_iam_options>{};
  if (aws_iam) {
    auto config = co_await aws_iam->evaluate(ctx);
    if (not config) {
      co_return None{};
    }
    auto parsed = aws_iam_options::from_record(
      located{std::move(*config), aws_iam->get_location()}, ctx.dh());
    if (not parsed) {
      co_return None{};
    }
    options = std::move(*parsed);
  }
  auto auth = resolve_aws_iam_auth(std::move(options), std::move(aws_region),
                                   ctx.dh(), requirement);
  if (not auth) {
    co_return None{};
  }
  co_return std::move(*auth);
}

} // namespace tenzir
