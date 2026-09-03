//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aws_credentials.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/logger.hpp>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/SSOCredentialsProvider.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleWithWebIdentityRequest.h>

#include <algorithm>
#include <chrono>
#include <mutex>

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
  if (auto profile = detail::getenv("AWS_PROFILE");
      profile and not profile->empty()) {
    config.profile = Aws::String{*profile};
  } else if (auto profile = detail::getenv("AWS_DEFAULT_PROFILE");
             profile and not profile->empty()) {
    config.profile = Aws::String{*profile};
  }
  return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>(
    config);
}

namespace {

/// Creates an STS client configuration with proper endpoint and proxy settings.
/// Caches environment variable lookups for efficiency.
auto make_sts_client_config(const Option<std::string>& region)
  -> Aws::Client::ClientConfiguration {
  // Cache environment variables to avoid repeated lookups.
  static const auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL");
  static const auto endpoint_url_sts = detail::getenv("AWS_ENDPOINT_URL_STS");
  auto config = Aws::Client::ClientConfiguration{};
  if (region) {
    config.region = *region;
  }
  config.allowSystemProxy = true;
  // STS-specific endpoint takes precedence.
  if (endpoint_url_sts) {
    config.endpointOverride = *endpoint_url_sts;
  } else if (endpoint_url) {
    config.endpointOverride = *endpoint_url;
  }
  return config;
}

class profile_credentials_provider_chain final
  : public Aws::Auth::AWSCredentialsProviderChain {
public:
  explicit profile_credentials_provider_chain(std::string const& profile) {
    AddProvider(
      std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
        profile.c_str()));
    AddProvider(std::make_shared<Aws::Auth::ProcessCredentialsProvider>(
      Aws::String{profile}));
    AddProvider(std::make_shared<Aws::Auth::SSOCredentialsProvider>(
      Aws::String{profile}));
  }
};

auto make_profile_aws_credentials_provider(std::string const& profile)
  -> std::shared_ptr<Aws::Auth::AWSCredentialsProvider> {
  return std::make_shared<profile_credentials_provider_chain>(profile);
}

auto get_profile_credentials(Aws::Auth::AWSCredentialsProvider& provider,
                             std::string_view profile)
  -> caf::expected<Aws::Auth::AWSCredentials> {
  auto creds = provider.GetAWSCredentials();
  if (creds.IsEmpty()) {
    return diagnostic::error("failed to load credentials from profile")
      .note("profile: {}", profile)
      .to_error();
  }
  if (creds.IsExpired()) {
    return diagnostic::error("loaded expired credentials from profile")
      .note("profile: {}", profile)
      .to_error();
  }
  return creds;
}

/// Custom credentials provider that automatically refreshes credentials
/// obtained via AssumeRoleWithWebIdentity. This is necessary for long-running
/// pipelines where the initial STS credentials would otherwise expire.
///
/// Features:
/// - Automatic refresh 5 minutes before expiration
/// - Retry with exponential backoff on transient failures
/// - Preserves old credentials on failure (they may still be valid)
/// - Maximum retry limit to prevent infinite loops
class web_identity_credentials_provider final
  : public Aws::Auth::AWSCredentialsProvider {
public:
  static constexpr auto max_consecutive_failures = 5;
  static constexpr auto initial_retry_delay = std::chrono::seconds{1};
  static constexpr auto max_retry_delay = std::chrono::seconds{60};

  web_identity_credentials_provider(resolved_web_identity web_identity,
                                    std::string role_arn,
                                    std::string session_name,
                                    Option<std::string> region,
                                    std::chrono::seconds refresh_buffer
                                    = std::chrono::minutes{5})
    : web_identity_{std::move(web_identity)},
      role_arn_{std::move(role_arn)},
      session_name_{std::move(session_name)},
      region_{std::move(region)},
      refresh_buffer_{refresh_buffer} {
  }

  auto GetAWSCredentials() -> Aws::Auth::AWSCredentials override {
    auto lock = std::unique_lock{mutex_};
    // Check if we need to refresh credentials.
    auto now = std::chrono::system_clock::now();
    if (credentials_.IsEmpty() or now >= expiration_ - refresh_buffer_) {
      // Check if we should retry based on backoff.
      if (consecutive_failures_ > 0 and now < next_retry_time_
          and not credentials_.IsEmpty() and now < expiration_) {
        // Keep serving the cached session only while it is still valid.
        TENZIR_DEBUG("web identity refresh in backoff, returning cached "
                     "credentials");
        return credentials_;
      }
      refresh_credentials();
    }
    return credentials_;
  }

private:
  auto refresh_credentials() -> void {
    TENZIR_VERBOSE("refreshing web identity credentials for role: {}",
                   role_arn_);
    // Fetch the web identity token.
    auto token = fetch_web_identity_token(web_identity_);
    if (not token) {
      handle_refresh_failure(
        fmt::format("failed to fetch web identity token: {}", token.error()));
      return;
    }
    // Create anonymous STS client with cached configuration.
    auto config = make_sts_client_config(region_);
    auto anonymous_credentials
      = std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>();
    auto sts_client
      = Aws::STS::STSClient{anonymous_credentials, nullptr, config};
    // Build and execute AssumeRoleWithWebIdentity request.
    auto request = Aws::STS::Model::AssumeRoleWithWebIdentityRequest{};
    request.SetRoleArn(role_arn_);
    request.SetRoleSessionName(session_name_.empty() ? "tenzir-session"
                                                     : session_name_);
    request.SetWebIdentityToken(*token);
    auto outcome = sts_client.AssumeRoleWithWebIdentity(request);
    if (not outcome.IsSuccess()) {
      handle_refresh_failure(fmt::format("failed to assume role with web "
                                         "identity: {}",
                                         outcome.GetError().GetMessage()));
      return;
    }
    // Success - update cached credentials and reset failure tracking.
    const auto& result = outcome.GetResult();
    const auto& creds = result.GetCredentials();
    credentials_ = Aws::Auth::AWSCredentials{
      creds.GetAccessKeyId(),
      creds.GetSecretAccessKey(),
      creds.GetSessionToken(),
    };
    // Convert AWS DateTime to std::chrono::system_clock::time_point.
    expiration_ = std::chrono::system_clock::from_time_t(
      static_cast<std::time_t>(creds.GetExpiration().SecondsWithMSPrecision()));
    consecutive_failures_ = 0;
    TENZIR_VERBOSE(
      "web identity credentials refreshed, expires at: {}",
      creds.GetExpiration().ToGmtString(Aws::Utils::DateFormat::ISO_8601));
  }

  auto handle_refresh_failure(std::string message) -> void {
    ++consecutive_failures_;
    last_error_ = std::move(message);
    // Calculate exponential backoff delay.
    auto delay = initial_retry_delay
                 * (1 << std::min(consecutive_failures_ - 1,
                                  max_consecutive_failures - 1));
    if (delay > max_retry_delay) {
      delay = max_retry_delay;
    }
    next_retry_time_ = std::chrono::system_clock::now() + delay;
    if (consecutive_failures_ >= max_consecutive_failures) {
      TENZIR_ERROR("{} (attempt {}/{}, max retries reached)", last_error_,
                   consecutive_failures_, max_consecutive_failures);
      // Preserve a still-valid cached session across transient STS outages.
      if (credentials_.IsEmpty()
          or std::chrono::system_clock::now() >= expiration_) {
        credentials_ = {};
      }
    } else {
      TENZIR_WARN(
        "{} (attempt {}/{}, retrying in {}s)", last_error_,
        consecutive_failures_, max_consecutive_failures,
        std::chrono::duration_cast<std::chrono::seconds>(delay).count());
      // Keep existing credentials - they may still be valid.
    }
  }

  resolved_web_identity web_identity_;
  std::string role_arn_;
  std::string session_name_;
  Option<std::string> region_;
  std::chrono::seconds refresh_buffer_;

  // AWS SDK credential callbacks are synchronous, so a blocking mutex is fine.
  mutable std::mutex mutex_;
  Aws::Auth::AWSCredentials credentials_;
  std::chrono::system_clock::time_point expiration_;
  int consecutive_failures_ = 0;
  std::chrono::system_clock::time_point next_retry_time_;
  std::string last_error_;

public:
  /// Returns the last error message from a failed credential refresh.
  auto last_error() const -> std::string {
    auto lock = std::unique_lock{mutex_};
    return last_error_;
  }
};

} // namespace

auto make_aws_credentials_provider(const Option<resolved_aws_credentials>& creds,
                                   const Option<std::string>& region)
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

  // Web identity + role: use auto-refreshing credentials provider.
  if (has_web_identity and has_role) {
    // Do an eager initial credential fetch so errors are returned through
    // the normal error path (which goes to the diagnostic handler) rather
    // than being logged to stderr during lazy refresh.
    auto provider = std::make_shared<web_identity_credentials_provider>(
      *creds->web_identity, creds->role, session_name, region);
    // Trigger initial credential fetch to surface any errors early.
    auto initial_creds = provider->GetAWSCredentials();
    if (initial_creds.IsEmpty()) {
      // Include the actual error message from the failed refresh.
      auto error = provider->last_error();
      auto diag = diagnostic::error("failed to obtain AWS credentials via web "
                                    "identity")
                    .note("role: {}", creds->role);
      if (not error.empty()) {
        diag = std::move(diag).note("{}", error);
      }
      return std::move(diag)
        .hint("check that the token endpoint is accessible and the role trust "
              "policy allows web identity federation")
        .to_error();
    }
    return provider;
  }
  if (has_explicit_creds and has_role) {
    // Explicit credentials + role: keep the base credentials provider so the
    // derived STS session can be refreshed.
    auto base_provider
      = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
        creds->access_key_id, creds->secret_access_key, creds->session_token);
    auto sts_config = make_sts_client_config(region);
    auto sts_client = std::make_shared<Aws::STS::STSClient>(
      base_provider, nullptr, sts_config);
    auto session = session_name.empty() ? "tenzir-session" : session_name;
    // The SDK also uses this value as AssumeRole's DurationSeconds. Preserve
    // the one-hour AWS default used by the previous direct request.
    constexpr auto session_duration_seconds = 3600;
    auto provider
      = std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
        creds->role, session, creds->external_id, session_duration_seconds,
        sts_client);
    auto assumed_creds = provider->GetAWSCredentials();
    if (assumed_creds.IsEmpty()) {
      return diagnostic::error("failed to assume role with explicit "
                               "credentials")
        .note("role ARN: {}", creds->role)
        .hint("check that the credentials can call `sts:AssumeRole`")
        .to_error();
    }
    if (assumed_creds.IsExpired()) {
      return diagnostic::error(
               "assume-role provider returned expired credentials")
        .note("role ARN: {}", creds->role)
        .to_error();
    }
    return provider;
  }
  if (has_explicit_creds) {
    // Explicit credentials only.
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      creds->access_key_id, creds->secret_access_key, creds->session_token);
  }
  if (has_profile and has_role) {
    // Profile + role: keep the profile provider chain so SSO-backed profiles
    // and refresh semantics survive the STS assume-role hop.
    auto base_provider = make_profile_aws_credentials_provider(creds->profile);
    auto base_creds = get_profile_credentials(*base_provider, creds->profile);
    if (not base_creds) {
      return base_creds.error();
    }
    auto sts_config = make_sts_client_config(region);
    auto sts_client = std::make_shared<Aws::STS::STSClient>(
      base_provider, nullptr, sts_config);
    auto session = session_name.empty() ? "tenzir-session" : session_name;
    auto provider
      = std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
        creds->role, session, creds->external_id,
        Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts_client);
    auto assumed_creds = provider->GetAWSCredentials();
    if (assumed_creds.IsEmpty()) {
      return diagnostic::error("failed to assume role with profile credentials")
        .note("profile: {}", creds->profile)
        .note("role ARN: {}", creds->role)
        .hint("check that the profile resolves credentials and can call "
              "`sts:AssumeRole`")
        .to_error();
    }
    if (assumed_creds.IsExpired()) {
      return diagnostic::error(
               "assume-role provider returned expired credentials")
        .note("profile: {}", creds->profile)
        .note("role ARN: {}", creds->role)
        .to_error();
    }
    return provider;
  }
  if (has_profile) {
    // Profile-based credentials only: keep the provider chain so SSO-backed
    // profiles can refresh credentials.
    auto provider = make_profile_aws_credentials_provider(creds->profile);
    auto profile_creds = get_profile_credentials(*provider, creds->profile);
    if (not profile_creds) {
      return profile_creds.error();
    }
    return provider;
  }
  if (has_role) {
    // Role assumption with default credentials - use auto-refreshing provider.
    auto sts_config = make_sts_client_config(region);
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
