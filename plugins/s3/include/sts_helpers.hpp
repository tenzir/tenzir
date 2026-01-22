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
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <caf/expected.hpp>

#include <optional>
#include <string>

namespace tenzir::plugins::s3 {

/// Holds temporary credentials from STS AssumeRole.
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

} // namespace tenzir::plugins::s3
