//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/diagnostics.hpp>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <caf/expected.hpp>

#include <optional>
#include <string>

namespace tenzir {

/// Holds temporary credentials from STS AssumeRole or profile loading.
struct sts_credentials {
  std::string access_key_id;
  std::string secret_access_key;
  std::string session_token;
};

/// Calls STS AssumeRole using base credentials and returns temporary
/// credentials.
inline auto
assume_role_with_credentials(const resolved_aws_credentials& base_creds,
                             const std::string& role_arn,
                             const std::string& session_name,
                             const std::string& external_id,
                             const std::optional<std::string>& region)
  -> caf::expected<sts_credentials> {
  // Create STS client configuration.
  auto config = Aws::Client::ClientConfiguration{};
  if (region) {
    config.region = *region;
  }
  // Honor proxy settings and endpoint overrides.
  config.allowSystemProxy = true;
  if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL")) {
    config.endpointOverride = *endpoint_url;
  }
  if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL_STS")) {
    config.endpointOverride = *endpoint_url;
  }
  // Create credentials provider from base credentials.
  auto base_credentials
    = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      base_creds.access_key_id, base_creds.secret_access_key,
      base_creds.session_token);
  // Create STS client.
  auto sts_client = Aws::STS::STSClient{base_credentials, nullptr, config};
  // Build AssumeRole request.
  auto request = Aws::STS::Model::AssumeRoleRequest{};
  request.SetRoleArn(role_arn);
  request.SetRoleSessionName(session_name.empty() ? "tenzir-session"
                                                  : session_name);
  if (not external_id.empty()) {
    request.SetExternalId(external_id);
  }
  // Perform the AssumeRole call.
  auto outcome = sts_client.AssumeRole(request);
  if (not outcome.IsSuccess()) {
    return diagnostic::error("failed to assume role")
      .note("role ARN: {}", role_arn)
      .note("{}", outcome.GetError().GetMessage())
      .to_error();
  }
  // Extract temporary credentials.
  const auto& creds = outcome.GetResult().GetCredentials();
  return sts_credentials{
    .access_key_id = creds.GetAccessKeyId(),
    .secret_access_key = creds.GetSecretAccessKey(),
    .session_token = creds.GetSessionToken(),
  };
}

/// Loads credentials from an AWS CLI profile.
inline auto load_profile_credentials(const std::string& profile)
  -> caf::expected<sts_credentials> {
  TENZIR_VERBOSE("using AWS profile {}", profile);
  auto provider = Aws::Auth::ProfileConfigFileAWSCredentialsProvider{
    profile.c_str(),
  };
  auto creds = provider.GetAWSCredentials();
  if (creds.IsEmpty()) {
    return diagnostic::error("failed to load credentials from profile")
      .note("profile: {}", profile)
      .to_error();
  }
  return sts_credentials{
    .access_key_id = creds.GetAWSAccessKeyId(),
    .secret_access_key = creds.GetAWSSecretKey(),
    .session_token = creds.GetSessionToken(),
  };
}

/// Creates an AWS credentials provider based on the resolved credentials.
///
/// This function implements the common credential resolution logic:
/// 1. If explicit credentials + role: assume role using explicit credentials
/// 2. If explicit credentials only: use them directly
/// 3. If profile + role: load profile credentials, then assume role
/// 4. If profile only: load profile credentials
/// 5. If role only: use STSAssumeRoleCredentialsProvider with default chain
/// 6. Otherwise: use default credential chain
///
/// @param creds Resolved AWS credentials (may be empty)
/// @param region Optional region for STS calls
/// @return Credentials provider or error
inline auto make_aws_credentials_provider(
  const std::optional<resolved_aws_credentials>& creds,
  const std::optional<std::string>& region)
  -> caf::expected<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>> {
  if (not creds) {
    return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
  }
  const auto has_explicit_creds = not creds->access_key_id.empty();
  const auto has_role = not creds->role.empty();
  const auto has_profile = not creds->profile.empty();
  // Get session_name from resolved credentials, default to empty.
  const auto session_name
    = creds->session_name.empty() ? std::string{} : creds->session_name;

  if (has_explicit_creds and has_role) {
    // Explicit credentials + role: use STS to assume role.
    auto sts_creds = assume_role_with_credentials(
      *creds, creds->role, session_name, creds->external_id, region);
    if (not sts_creds) {
      return sts_creds.error();
    }
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      sts_creds->access_key_id, sts_creds->secret_access_key,
      sts_creds->session_token);
  }
  if (has_explicit_creds) {
    // Explicit credentials only.
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      creds->access_key_id, creds->secret_access_key, creds->session_token);
  }
  if (has_profile and has_role) {
    // Profile + role: load profile credentials, then assume role.
    auto profile_creds = load_profile_credentials(creds->profile);
    if (not profile_creds) {
      return profile_creds.error();
    }
    auto base_creds = resolved_aws_credentials{
      .region = {},
      .profile = {},
      .session_name = {},
      .access_key_id = profile_creds->access_key_id,
      .secret_access_key = profile_creds->secret_access_key,
      .session_token = profile_creds->session_token,
      .role = {},
      .external_id = {},
    };
    auto sts_creds = assume_role_with_credentials(
      base_creds, creds->role, session_name, creds->external_id, region);
    if (not sts_creds) {
      return sts_creds.error();
    }
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      sts_creds->access_key_id, sts_creds->secret_access_key,
      sts_creds->session_token);
  }
  if (has_profile) {
    // Profile-based credentials only.
    auto profile_creds = load_profile_credentials(creds->profile);
    if (not profile_creds) {
      return profile_creds.error();
    }
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      profile_creds->access_key_id, profile_creds->secret_access_key,
      profile_creds->session_token);
  }
  if (has_role) {
    // Role assumption with default credentials - use auto-refreshing provider.
    auto sts_config = Aws::Client::ClientConfiguration{};
    if (region) {
      sts_config.region = *region;
    }
    sts_config.allowSystemProxy = true;
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL")) {
      sts_config.endpointOverride = *endpoint_url;
    }
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL_STS")) {
      sts_config.endpointOverride = *endpoint_url;
    }
    auto base_credentials
      = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    auto sts_client = std::make_shared<Aws::STS::STSClient>(
      base_credentials, nullptr, sts_config);
    auto session = session_name.empty() ? "tenzir-session" : session_name;
    return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
      creds->role, session, creds->external_id,
      Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts_client);
  }
  // Default credential chain.
  return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

} // namespace tenzir
