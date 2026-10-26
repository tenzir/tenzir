//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/option.hpp"
#include "tenzir/startup_argument.hpp"
#include "tenzir/web_identity.hpp"

#include <string>
#include <utility>
#include <vector>

namespace tenzir {

class OpCtx;

// TODO: Move to the shared Amazon module as `tenzir::amazon`.
/// Resolved AWS credentials for use with AWS SDK clients.
struct resolved_aws_credentials {
  std::string region;
  std::string profile;
  std::string session_name;
  std::string access_key_id;
  std::string secret_access_key;
  std::string session_token;
  std::string role;
  std::string external_id;
  Option<resolved_web_identity> web_identity;
};

// TODO: Move to the shared Amazon module as `tenzir::amazon::AwsIamOptions`.
/// AWS IAM authentication options.
///
/// This struct provides common AWS authentication configuration that can be
/// used across different AWS-related operators (SQS, S3, Kafka MSK, etc.).
struct aws_iam_options {
  /// AWS region for API requests (optional, SDK uses default resolution).
  Option<nova::Secret> region;
  /// AWS CLI profile name to use for credentials.
  Option<nova::Secret> profile;
  /// IAM role ARN to assume.
  Option<nova::Secret> role;
  /// Session name for role assumption.
  Option<nova::Secret> session_name;
  /// External ID for role assumption.
  Option<nova::Secret> external_id;
  /// AWS access key ID.
  Option<nova::Secret> access_key_id;
  /// AWS secret access key.
  Option<nova::Secret> secret_access_key;
  /// AWS session token for temporary credentials.
  Option<nova::Secret> session_token;
  /// Web identity configuration for OIDC-based authentication.
  Option<web_identity_options> web_identity;
  /// Source location for diagnostics.
  location loc;

  /// Parses AWS IAM options from an evaluated TQL record.
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
  /// - `web_identity`: Web identity configuration for OIDC authentication
  static auto from_record(located<nova::Record> config, diagnostic_handler& dh)
    -> failure_or<aws_iam_options>;

  /// Returns true if explicit credentials are configured.
  auto has_explicit_credentials() const -> bool {
    return access_key_id.has_value();
  }
};

// TODO: Move to the shared Amazon module as `tenzir::amazon`.
/// Describes whether `aws_region` is mandatory for a configured IAM block.
/// Kafka MSK IAM auth needs an explicit region because SigV4 token signing
/// includes the region in the signing scope.
enum class AwsIamRegionRequirement {
  optional,
  required_with_iam,
};

// TODO: Move to the shared Amazon module as `tenzir::amazon`.
/// Holds parsed IAM options and the resolved credentials.
struct ResolvedAwsIamAuth {
  Option<aws_iam_options> options;
  Option<resolved_aws_credentials> credentials;
};

// TODO: Move to the shared Amazon module as `tenzir::amazon`.
/// Resolves already-parsed AWS IAM options into runtime auth state.
///
/// This function validates region requirements and reveals the credentials
/// from the options and `aws_region`. A region in the options takes
/// precedence.
auto resolve_aws_iam_auth(Option<aws_iam_options> aws_iam,
                          Option<located<std::string>> aws_region,
                          diagnostic_handler& dh,
                          AwsIamRegionRequirement requirement
                          = AwsIamRegionRequirement::optional)
  -> failure_or<ResolvedAwsIamAuth>;

/// Evaluates the optional `aws_iam` argument at startup and resolves runtime
/// auth state from it.
auto resolve_aws_iam_auth(Option<StartupArgument<nova::Record>> const& aws_iam,
                          Option<located<std::string>> aws_region, OpCtx& ctx,
                          AwsIamRegionRequirement requirement
                          = AwsIamRegionRequirement::optional)
  -> Task<Option<ResolvedAwsIamAuth>>;

} // namespace tenzir
