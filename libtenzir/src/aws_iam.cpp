//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/aws_iam.hpp"

#include "tenzir/diagnostics.hpp"

#include <algorithm>
#include <array>

namespace tenzir {

auto aws_iam_options::from_record(located<record> config,
                                  diagnostic_handler& dh)
  -> failure_or<aws_iam_options> {
  constexpr auto known = std::array{
    "region",      "profile",       "assume_role",       "session_name",
    "external_id", "access_key_id", "secret_access_key", "session_token",
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
  const auto assign_non_empty_string
    = [&](std::string_view key, auto&& x) -> failure_or<void> {
    if (auto it = config.inner.find(key); it != config.inner.end()) {
      auto* extracted = try_as<std::string>(it->second.get_data());
      if (not extracted) {
        diagnostic::error("'{}' must be a `string`", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      if (extracted->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x = std::move(*extracted);
    }
    return {};
  };
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
  TRY(assign_non_empty_string("region", opts.region));
  TRY(assign_non_empty_string("profile", opts.profile));
  TRY(assign_secret("assume_role", opts.role));
  TRY(assign_non_empty_string("session_name", opts.session_name));
  TRY(assign_secret("external_id", opts.external_id));
  TRY(assign_secret("access_key_id", opts.access_key_id));
  TRY(assign_secret("secret_access_key", opts.secret_access_key));
  TRY(assign_secret("session_token", opts.session_token));
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
  return opts;
}

auto aws_iam_options::make_secret_requests(resolved_aws_credentials& resolved,
                                           diagnostic_handler& dh) const
  -> std::vector<secret_request> {
  auto requests = std::vector<secret_request>{};
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
  return requests;
}

} // namespace tenzir
