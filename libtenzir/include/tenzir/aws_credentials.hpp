//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <tenzir/aws_iam.hpp>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <caf/expected.hpp>

#include <memory>
#include <string>

namespace tenzir {

// TODO: Move to the shared Amazon module as `tenzir::amazon`.
/// Creates the default AWS credential provider chain with bounded IMDS latency.
///
/// The returned chain keeps IMDS enabled but constrains metadata lookups to a
/// low-latency profile suitable for interactive/operator paths.
auto make_default_aws_credentials_provider_chain()
  -> std::shared_ptr<Aws::Auth::AWSCredentialsProvider>;

// TODO: Move to the shared Amazon module as `tenzir::amazon`.
/// Creates an AWS credentials provider based on the resolved credentials.
///
/// This function implements the common credential resolution logic:
/// 1. If web_identity + role: fetch token and call AssumeRoleWithWebIdentity
/// 2. If explicit credentials + role: assume role using explicit credentials
/// 3. If explicit credentials only: use them directly
/// 4. If profile + role: load profile credentials, then assume role
/// 5. If profile only: load profile credentials
/// 6. If role only: use STSAssumeRoleCredentialsProvider with default chain
/// 7. Otherwise: use default credential chain
///
/// @param creds Resolved AWS credentials (may be empty)
/// @param region Optional region for STS calls
/// @return Credentials provider or error
auto make_aws_credentials_provider(const Option<resolved_aws_credentials>& creds,
                                   const Option<std::string>& region)
  -> caf::expected<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>>;

} // namespace tenzir
