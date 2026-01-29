//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aws_credentials.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/load_contents.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/transfer.hpp>
#include <tenzir/try_simdjson.hpp>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <aws/sts/model/AssumeRoleWithWebIdentityRequest.h>
#include <simdjson.h>

namespace tenzir {

auto make_default_aws_credentials_provider_chain()
  -> std::shared_ptr<Aws::Auth::AWSCredentialsProvider> {
  auto config
    = Aws::Client::ClientConfiguration::CredentialProviderConfiguration{};
  // Bound IMDS latency in interactive paths while keeping EC2 instance-profile
  // credentials available when present.
  config.imdsConfig.metadataServiceTimeout = 1;
  config.imdsConfig.metadataServiceNumAttempts = 1;
  config.imdsConfig.disableImdsV1 = true;
  config.imdsConfig.disableImds = false;
  return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>(
    config);
}

auto assume_role_with_credentials(const resolved_aws_credentials& base_creds,
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

auto load_profile_credentials(const std::string& profile)
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

auto fetch_web_identity_token(const resolved_web_identity& web_identity)
  -> caf::expected<std::string> {
  // Case 1: Direct token value.
  if (not web_identity.token.empty()) {
    TENZIR_VERBOSE("using direct web identity token");
    return web_identity.token;
  }
  // Case 2: Token from file.
  if (not web_identity.token_file.empty()) {
    TENZIR_VERBOSE("reading web identity token from file: {}",
                   web_identity.token_file);
    auto contents = detail::load_contents(web_identity.token_file);
    if (not contents) {
      return diagnostic::error("failed to read web identity token file")
        .note("file: {}", web_identity.token_file)
        .note("{}", contents.error())
        .to_error();
    }
    // Trim whitespace from token.
    return detail::trim(*contents);
  }
  // Case 3: Token from HTTP endpoint.
  if (not web_identity.token_endpoint.empty()) {
    TENZIR_VERBOSE("fetching web identity token from endpoint: {}",
                   web_identity.token_endpoint);
    auto xfer = transfer{};
    auto req = http::request{};
    req.uri = web_identity.token_endpoint;
    req.method = "GET";
    // Add custom headers.
    for (const auto& [name, value] : web_identity.headers) {
      req.headers.emplace_back(name, value);
    }
    if (auto err = xfer.prepare(req); err) {
      return diagnostic::error("failed to prepare web identity token request")
        .note("{}", err)
        .to_error();
    }
    // Collect response body.
    auto body = std::string{};
    for (auto&& chunk : xfer.download_chunks()) {
      if (not chunk) {
        return diagnostic::error("failed to fetch web identity token")
          .note("{}", chunk.error())
          .to_error();
      }
      if (*chunk) {
        body.append(reinterpret_cast<const char*>((*chunk)->data()),
                    (*chunk)->size());
      }
    }
    // Check if token_path is set (JSON response) or nullopt (plain text).
    if (not web_identity.token_path) {
      // Plain text response: return trimmed body.
      TENZIR_VERBOSE("treating web identity token response as plain text");
      return detail::trim(body);
    }
    // JSON response: extract token using JSON path.
    TENZIR_VERBOSE("extracting web identity token from JSON path: {}",
                   *web_identity.token_path);
    // Simple JSON path extraction. For now, support only single-level paths
    // like ".access_token" or ".token".
    auto path = *web_identity.token_path;
    if (path.starts_with('.')) {
      path = path.substr(1);
    }
    auto parser = simdjson::ondemand::parser{};
    auto padded = simdjson::padded_string{body};
    auto doc = parser.iterate(padded);
    if (doc.error() != simdjson::SUCCESS) {
      return diagnostic::error("failed to parse web identity token response as "
                               "JSON")
        .note("error: {}", simdjson::error_message(doc.error()))
        .to_error();
    }
    auto token_value = doc[path];
    if (token_value.error() != simdjson::SUCCESS) {
      return diagnostic::error("failed to extract token from JSON response")
        .note("path: {}", *web_identity.token_path)
        .note("error: {}", simdjson::error_message(token_value.error()))
        .to_error();
    }
    auto token_str = token_value.get_string();
    if (token_str.error() != simdjson::SUCCESS) {
      return diagnostic::error("web identity token is not a string")
        .note("path: {}", *web_identity.token_path)
        .to_error();
    }
    return std::string{token_str.value()};
  }
  // Should not reach here if validation was correct.
  return diagnostic::error("no web identity token source configured")
    .to_error();
}

auto assume_role_with_web_identity(const std::string& role_arn,
                                   const std::string& session_name,
                                   const std::string& web_identity_token,
                                   const std::optional<std::string>& region)
  -> caf::expected<sts_credentials> {
  TENZIR_VERBOSE("assuming role with web identity: {}", role_arn);
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
  // Create an anonymous credentials provider since AssumeRoleWithWebIdentity
  // doesn't require base credentials - the web identity token is the
  // authentication.
  auto anonymous_credentials
    = std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>();
  // Create STS client with anonymous credentials.
  auto sts_client
    = Aws::STS::STSClient{anonymous_credentials, nullptr, config};
  // Build AssumeRoleWithWebIdentity request.
  auto request = Aws::STS::Model::AssumeRoleWithWebIdentityRequest{};
  request.SetRoleArn(role_arn);
  request.SetRoleSessionName(session_name.empty() ? "tenzir-session"
                                                  : session_name);
  request.SetWebIdentityToken(web_identity_token);
  // Perform the AssumeRoleWithWebIdentity call.
  auto outcome = sts_client.AssumeRoleWithWebIdentity(request);
  if (not outcome.IsSuccess()) {
    return diagnostic::error("failed to assume role with web identity")
      .note("role ARN: {}", role_arn)
      .note("{}", outcome.GetError().GetMessage())
      .to_error();
  }
  // Extract temporary credentials.
  const auto& creds = outcome.GetResult().GetCredentials();
  TENZIR_VERBOSE("successfully assumed role with web identity");
  return sts_credentials{
    .access_key_id = creds.GetAccessKeyId(),
    .secret_access_key = creds.GetSecretAccessKey(),
    .session_token = creds.GetSessionToken(),
  };
}

auto make_aws_credentials_provider(
  const std::optional<resolved_aws_credentials>& creds,
  const std::optional<std::string>& region)
  -> caf::expected<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>> {
  if (not creds) {
    return make_default_aws_credentials_provider_chain();
  }
  const auto has_explicit_creds = not creds->access_key_id.empty();
  const auto has_role = not creds->role.empty();
  const auto has_profile = not creds->profile.empty();
  const auto has_web_identity = creds->web_identity.has_value();
  // Get session_name from resolved credentials, default to empty.
  const auto session_name
    = creds->session_name.empty() ? std::string{} : creds->session_name;

  // Web identity + role: fetch token and call AssumeRoleWithWebIdentity.
  if (has_web_identity and has_role) {
    auto token = fetch_web_identity_token(*creds->web_identity);
    if (not token) {
      return token.error();
    }
    auto sts_creds = assume_role_with_web_identity(creds->role, session_name,
                                                   *token, region);
    if (not sts_creds) {
      return sts_creds.error();
    }
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      sts_creds->access_key_id, sts_creds->secret_access_key,
      sts_creds->session_token);
  }
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
      .web_identity = {},
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
    auto base_credentials = make_default_aws_credentials_provider_chain();
    auto sts_client = std::make_shared<Aws::STS::STSClient>(
      base_credentials, nullptr, sts_config);
    auto session = session_name.empty() ? "tenzir-session" : session_name;
    return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
      creds->role, session, creds->external_id,
      Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts_client);
  }
  // Default credential chain.
  return make_default_aws_credentials_provider_chain();
}

} // namespace tenzir
