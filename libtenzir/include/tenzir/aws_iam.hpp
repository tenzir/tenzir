//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"

#include <optional>
#include <string>
#include <vector>

namespace tenzir {

/// Resolved AWS credentials for use with AWS SDK clients.
struct resolved_aws_credentials {
  std::string access_key_id;
  std::string secret_access_key;
  std::string session_token;
};

/// AWS IAM authentication options.
///
/// This struct provides common AWS authentication configuration that can be
/// used across different AWS-related operators (SQS, S3, Kafka MSK, etc.).
struct aws_iam_options {
  /// AWS region for API requests (optional, SDK uses default resolution).
  std::optional<std::string> region;
  /// AWS CLI profile name to use for credentials.
  std::optional<std::string> profile;
  /// IAM role ARN to assume.
  std::optional<std::string> role;
  /// Session name for role assumption.
  std::optional<std::string> session_name;
  /// External ID for role assumption.
  std::optional<std::string> ext_id;
  /// AWS access key ID.
  std::optional<secret> access_key_id;
  /// AWS secret access key.
  std::optional<secret> secret_access_key;
  /// AWS session token for temporary credentials.
  std::optional<secret> session_token;
  /// Source location for diagnostics.
  location loc;

  friend auto inspect(auto& f, aws_iam_options& x) -> bool {
    return f.object(x).fields(
      f.field("region", x.region), f.field("profile", x.profile),
      f.field("role", x.role), f.field("session_name", x.session_name),
      f.field("ext_id", x.ext_id), f.field("access_key_id", x.access_key_id),
      f.field("secret_access_key", x.secret_access_key),
      f.field("session_token", x.session_token), f.field("loc", x.loc));
  }

  /// Parses AWS IAM options from a TQL record.
  ///
  /// Recognized keys:
  /// - `region`: AWS region for API requests (optional)
  /// - `profile`: AWS CLI profile name to use for credentials
  /// - `access_key_id`: AWS access key ID
  /// - `secret_access_key`: AWS secret access key
  /// - `session_token`: AWS session token for temporary credentials
  /// - `assume_role`: IAM role ARN to assume
  /// - `session_name`: Session name for role assumption
  /// - `external_id`: External ID for role assumption
  static auto from_record(located<record> config, diagnostic_handler& dh)
    -> failure_or<aws_iam_options>;

  /// Creates secret requests for resolving credentials.
  ///
  /// @param resolved Output struct to store resolved credentials
  /// @param dh Diagnostic handler for errors
  /// @return Vector of secret requests to pass to resolve_secrets_must_yield
  auto make_secret_requests(resolved_aws_credentials& resolved,
                            diagnostic_handler& dh) const
    -> std::vector<secret_request>;

  /// Returns true if explicit credentials are configured.
  auto has_explicit_credentials() const -> bool {
    return access_key_id.has_value();
  }
};

} // namespace tenzir
