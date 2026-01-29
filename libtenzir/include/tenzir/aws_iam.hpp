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
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace tenzir {

class OpCtx;

/// Resolved web identity token configuration.
struct resolved_web_identity {
  std::string token_endpoint;
  std::string token_file;
  std::string token;
  std::vector<std::pair<std::string, std::string>> headers;
  /// JSON path to extract the token from the endpoint response.
  /// nullopt means the response is plain text (no JSON parsing).
  std::optional<std::string> token_path;
};
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
  std::optional<resolved_web_identity> web_identity;
};

/// Web identity token configuration for OIDC-based authentication.
///
/// Supports fetching OIDC tokens from:
/// - HTTP endpoint (e.g., Azure IMDS, GCP metadata server)
/// - File path (e.g., Kubernetes service account token)
/// - Direct token value
struct web_identity_options {
  /// HTTP endpoint to fetch the token from.
  std::optional<secret> token_endpoint;
  /// File path containing the token.
  std::optional<secret> token_file;
  /// Direct token value.
  std::optional<secret> token;
  /// HTTP headers for the token endpoint request.
  /// Each value can be a string or secret.
  std::optional<std::vector<std::pair<std::string, secret>>> headers;
  /// JSON path to extract the token from endpoint response.
  /// Defaults to ".access_token". Set to null for plain text responses.
  std::optional<std::string> token_path;
  /// True if token_path was explicitly set to null (plain text response).
  bool token_path_is_null = false;
  /// Source location for diagnostics.
  location loc;

  friend auto inspect(auto& f, web_identity_options& x) -> bool {
    return f.object(x).fields(
      f.field("token_endpoint", x.token_endpoint),
      f.field("token_file", x.token_file), f.field("token", x.token),
      f.field("headers", x.headers), f.field("token_path", x.token_path),
      f.field("token_path_is_null", x.token_path_is_null),
      f.field("loc", x.loc));
  }

  /// Parses web identity options from a TQL record.
  ///
  /// Recognized keys:
  /// - `token_endpoint`: HTTP endpoint URL to fetch the token
  /// - `token_file`: File path containing the token
  /// - `token`: Direct token value
  /// - `headers`: HTTP headers for endpoint requests
  /// - `token_path`: JSON path to extract token (default: ".access_token")
  static auto from_record(located<record> config, diagnostic_handler& dh)
    -> failure_or<web_identity_options>;
};

/// AWS IAM authentication options.
///
/// This struct provides common AWS authentication configuration that can be
/// used across different AWS-related operators (SQS, S3, Kafka MSK, etc.).
struct aws_iam_options {
  /// AWS region for API requests (optional, SDK uses default resolution).
  std::optional<secret> region;
  /// AWS CLI profile name to use for credentials.
  std::optional<secret> profile;
  /// IAM role ARN to assume.
  std::optional<secret> role;
  /// Session name for role assumption.
  std::optional<secret> session_name;
  /// External ID for role assumption.
  std::optional<secret> external_id;
  /// AWS access key ID.
  std::optional<secret> access_key_id;
  /// AWS secret access key.
  std::optional<secret> secret_access_key;
  /// AWS session token for temporary credentials.
  std::optional<secret> session_token;
  /// Web identity configuration for OIDC-based authentication.
  std::optional<web_identity_options> web_identity;
  /// Source location for diagnostics.
  location loc;

  friend auto inspect(auto& f, aws_iam_options& x) -> bool {
    return f.object(x).fields(
      f.field("region", x.region), f.field("profile", x.profile),
      f.field("role", x.role), f.field("session_name", x.session_name),
      f.field("external_id", x.external_id),
      f.field("access_key_id", x.access_key_id),
      f.field("secret_access_key", x.secret_access_key),
      f.field("session_token", x.session_token),
      f.field("web_identity", x.web_identity), f.field("loc", x.loc));
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
  /// - `web_identity`: Web identity configuration for OIDC authentication
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

/// Describes whether `aws_region` is mandatory for a configured IAM block.
/// Kafka MSK IAM auth needs an explicit region because SigV4 token signing
/// includes the region in the signing scope.
enum class AwsIamRegionRequirement {
  optional,
  required_with_iam,
};

/// Holds parsed IAM options, credential slots, and pending secret requests.
struct ResolvedAwsIamAuth {
  std::optional<aws_iam_options> options;
  std::optional<resolved_aws_credentials> credentials;
};

/// Resolves already-parsed AWS IAM options into runtime auth state.
///
/// This function:
/// 1. validates region requirements,
/// 2. allocates a credential container when needed,
/// 3. collects secret requests for later resolution by the caller.
auto resolve_aws_iam_auth(std::optional<aws_iam_options> aws_iam,
                          std::optional<located<std::string>> aws_region,
                          diagnostic_handler& dh,
                          AwsIamRegionRequirement requirement
                          = AwsIamRegionRequirement::optional)
  -> failure_or<ResolvedAwsIamAuth>;

/// Parses optional `aws_iam` input and then resolves runtime auth state.
auto resolve_aws_iam_auth(std::optional<located<record>> aws_iam,
                          std::optional<located<std::string>> aws_region,
                          diagnostic_handler& dh,
                          AwsIamRegionRequirement requirement
                          = AwsIamRegionRequirement::optional)
  -> failure_or<ResolvedAwsIamAuth>;

/// Resolves AWS IAM auth and applies secret resolution through `ctx`.
auto resolve_aws_iam_auth(std::optional<located<record>> aws_iam,
                          std::optional<located<std::string>> aws_region,
                          OpCtx& ctx,
                          AwsIamRegionRequirement requirement
                          = AwsIamRegionRequirement::optional)
  -> Task<std::optional<ResolvedAwsIamAuth>>;

} // namespace tenzir
