//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/aws_iam.hpp>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <caf/expected.hpp>

#include <memory>
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
auto assume_role_with_credentials(const resolved_aws_credentials& base_creds,
                                  const std::string& role_arn,
                                  const std::string& session_name,
                                  const std::string& external_id,
                                  const std::optional<std::string>& region)
  -> caf::expected<sts_credentials>;

/// Loads credentials from an AWS CLI profile.
auto load_profile_credentials(const std::string& profile)
  -> caf::expected<sts_credentials>;

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
auto make_aws_credentials_provider(
  const std::optional<resolved_aws_credentials>& creds,
  const std::optional<std::string>& region)
  -> caf::expected<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>>;

} // namespace tenzir
