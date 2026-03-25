//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"

#include "tenzir/aws_credentials.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/diagnostics.hpp"

#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/error.hpp>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/sts/STSClient.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>

#include <string_view>

namespace tenzir::plugins::kafka {

auto configuration::aws_iam_callback::oauthbearer_token_refresh_cb(
  RdKafka::Handle* handle, const std::string&) -> void {
  const auto valid_for = std::chrono::seconds{900};
  // Region is required for Kafka MSK - validated at parse time.
  // Use resolved region from creds_ since options_.region is a secret.
  TENZIR_ASSERT(creds_ and not creds_->region.empty());
  const auto& region = creds_->region;
  const auto has_explicit_creds = not creds_->access_key_id.empty();
  const auto has_profile = not creds_->profile.empty();
  const auto has_role = not creds_->role.empty();
  const auto auth_mode = [&]() -> const char* {
    if (has_explicit_creds and has_role) {
      return "explicit+assume_role";
    }
    if (has_profile and has_role) {
      return "profile+assume_role";
    }
    if (has_role) {
      return "default+assume_role";
    }
    if (has_explicit_creds) {
      return "explicit";
    }
    if (has_profile) {
      return "profile";
    }
    return "default_chain";
  }();
  const auto url = Aws::Http::URI{
    fmt::format("https://kafka.{}.amazonaws.com/", region),
  };
  auto request = Aws::Http::Standard::StandardHttpRequest{
    url,
    Aws::Http::HttpMethod::HTTP_GET,
  };
  const auto emit_credentials_unavailable = [&](std::string_view reason) {
    auto const is_truthy = [](std::string_view value) {
      return value == "1" or value == "true" or value == "TRUE"
             or value == "True";
    };
    auto out
      = diagnostic::warning("failed to refresh AWS credentials for Kafka IAM")
          .primary(options_.loc.subloc(0, 1))
          .note("reason={}", reason)
          .note("aws_iam.mode={}", auth_mode)
          .note("aws_iam.region={}", region);
    if (has_profile) {
      out = std::move(out).note("aws_iam.profile={}", creds_->profile);
    }
    if (has_role) {
      out = std::move(out).note("aws_iam.assume_role={}", creds_->role);
      out = std::move(out).hint(
        "verify the role ARN exists, the source credentials can call "
        "`sts:AssumeRole`, and the role trust policy allows that principal");
    } else {
      out = std::move(out).hint(
        "verify base credentials resolve (for example with "
        "`aws sts get-caller-identity`) for the configured profile or "
        "default chain");
    }
    if (not has_explicit_creds and not has_profile) {
      if (auto disabled = detail::getenv("AWS_EC2_METADATA_DISABLED")) {
        out = std::move(out).note("AWS_EC2_METADATA_DISABLED={}", *disabled);
        if (is_truthy(*disabled)) {
          out = std::move(out).hint(
            "if you run on EC2, unset `AWS_EC2_METADATA_DISABLED` so IMDS "
            "credentials are available to the default chain");
        }
      }
      if (auto profile = detail::getenv("AWS_PROFILE")) {
        out = std::move(out).note("AWS_PROFILE={}", *profile);
      }
    }
    std::move(out).emit(dh_);
  };
  const auto report_refresh_failure = [&](std::string_view reason) {
    auto err = handle->oauthbearer_set_token_failure(std::string{reason});
    if (err != RdKafka::ERR_NO_ERROR) {
      diagnostic::warning("failed to report oauth token refresh failure")
        .note("librdkafka error: {}", RdKafka::err2str(err))
        .primary(options_.loc.subloc(0, 1))
        .emit(dh_);
    }
  };
  // Use the shared credential provider which handles all auth methods including
  // web identity.
  auto provider_result
    = tenzir::make_aws_credentials_provider(creds_, std::optional{region});
  if (not provider_result) {
    diagnostic::error(provider_result.error()).emit(dh_);
    auto err
      = handle->oauthbearer_set_token_failure("failed to create credentials "
                                              "provider");
    if (err != RdKafka::ERR_NO_ERROR) {
      diagnostic::error("failed to set oauthbearer token failure")
        .note("{}", RdKafka::err2str(err))
        .emit(dh_);
    }
    return;
  }
  auto provider = std::move(*provider_result);
  if (auto creds = provider->GetAWSCredentials(); creds.IsEmpty()) {
    emit_credentials_unavailable(has_role
                                   ? "assume-role provider returned empty "
                                     "credentials"
                                   : "provider returned empty credentials");
    report_refresh_failure(fmt::format(
      "empty AWS credentials (mode={}, region={})", auth_mode, region));
    return;
  } else if (creds.IsExpired()) {
    emit_credentials_unavailable(has_role
                                   ? "assume-role provider returned expired "
                                     "credentials"
                                   : "provider returned expired credentials");
    report_refresh_failure(fmt::format(
      "expired AWS credentials (mode={}, region={})", auth_mode, region));
    return;
  }
  request.AddQueryStringParameter("Action", "kafka-cluster:Connect");
  const auto signer = Aws::Client::AWSAuthV4Signer{
    provider,
    "kafka-cluster",
    region,
  };
  TENZIR_ASSERT(signer.PresignRequest(request, valid_for.count()));
  request.AddQueryStringParameter("User-Agent", "Tenzir");
  auto encoded = detail::base64::encode(request.GetURIString());
  boost::trim_right_if(encoded, [](auto&& x) {
    return x == '=';
  });
  // NOTE: This is necessary to get a URL-safe encoding as these characters have
  // special meaning in URLs.
  // See:
  // https://github.com/aws/aws-msk-iam-sasl-signer-python/blob/84fb289b256c8551183cb006b68a6e757d7cb467/aws_msk_iam_sasl_signer/MSKAuthTokenProvider.py#L238-L240
  boost::replace_all(encoded, "+", "-");
  boost::replace_all(encoded, "/", "_");
  auto errstr = std::string{};
  // TODO: Maybe use the credential expiration time instead?
  const auto expiration = duration_cast<std::chrono::milliseconds>(
    (time::clock::now() + valid_for).time_since_epoch());
  handle->oauthbearer_set_token(encoded, expiration.count(), "Tenzir", {},
                                errstr);
  if (not errstr.empty()) {
    diagnostic::error("failed to set oauth token: {}", errstr).emit(dh_);
  }
}

auto configuration::error_callback::event_cb(RdKafka::Event& event) -> void {
  const auto get_severity = [&] {
    switch (event.severity()) {
      case RdKafka::Event::EVENT_SEVERITY_EMERG:
        return "emergency";
      case RdKafka::Event::EVENT_SEVERITY_ALERT:
        return "alert";
      case RdKafka::Event::EVENT_SEVERITY_CRITICAL:
        return "critical";
      case RdKafka::Event::EVENT_SEVERITY_ERROR:
        return "error";
      case RdKafka::Event::EVENT_SEVERITY_WARNING:
        return "warning";
      case RdKafka::Event::EVENT_SEVERITY_NOTICE:
        return "notice";
      case RdKafka::Event::EVENT_SEVERITY_INFO:
        return "info";
      case RdKafka::Event::EVENT_SEVERITY_DEBUG:
        return "debug";
      default:
        return "unknown";
    }
  };
  const auto error_code = event.err();
  const auto error_msg = event.str();
  if (event.type() == RdKafka::Event::EVENT_ERROR
      or event.severity() >= RdKafka::Event::EVENT_SEVERITY_WARNING) {
    diagnostic::warning("librdkafka {}: {} ({})", get_severity(),
                        error_msg.empty() ? RdKafka::err2str(error_code)
                                          : error_msg,
                        std::to_underlying(error_code))
      .severity(event.fatal() ? severity::error : severity::warning)
      .emit(dh_);
  }
}

auto configuration::make(const record& options,
                         std::optional<aws_iam_options> aws,
                         std::optional<resolved_aws_credentials> creds,
                         diagnostic_handler& dh)
  -> caf::expected<configuration> {
  configuration result;
  if (auto err = result.set(options); err.valid()) {
    return err;
  }
  auto errstr = std::string{};
  result.error_callback_ = std::make_shared<error_callback>(dh);
  result.conf_->set("event_cb", result.error_callback_.get(), errstr);
  if (not errstr.empty()) {
    return diagnostic::error("failed to set event callback: {}", errstr)
      .to_error();
  }
  if (aws) {
    result.aws_ = std::make_shared<aws_iam_callback>(std::move(aws).value(),
                                                     std::move(creds), dh);
    result.conf_->set("oauthbearer_token_refresh_cb", result.aws_.get(),
                      errstr);
    if (not errstr.empty()) {
      return diagnostic::error("failed to set oauth callback: {}", errstr)
        .to_error();
    }
  }
  return result;
}

auto configuration::get(std::string_view key) const
  -> caf::expected<std::string> {
  std::string value;
  auto result = conf_->get(std::string{key}, value);
  if (result != RdKafka::Conf::ConfResult::CONF_OK) {
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to get key: {}", key));
  }
  return value;
}

auto configuration::set(const std::string& key, const std::string& value)
  -> caf::error {
  std::string error;
  auto result = conf_->set(key, value, error);
  switch (result) {
    case RdKafka::Conf::ConfResult::CONF_UNKNOWN:
      return caf::make_error(ec::unspecified,
                             fmt::format("unknown configuration property: {}",
                                         error));
    case RdKafka::Conf::ConfResult::CONF_INVALID:
      return caf::make_error(
        ec::unspecified, fmt::format("invalid configuration value: {}", error));
    case RdKafka::Conf::ConfResult::CONF_OK:
      break;
  }
  return {};
}

auto configuration::set(const record& options) -> caf::error {
  auto stringify = detail::overload{
    [](const auto& x) {
      return to_string(x);
    },
    [](const std::string& x) {
      return x;
    },
  };
  for (const auto& [key, value] : options) {
    if (auto err = set(key, tenzir::match(value, stringify)); err.valid()) {
      return err;
    }
  }
  return {};
}

auto configuration::set_rebalance_cb(int64_t offset) -> caf::error {
  rebalance_callback_ = std::make_shared<rebalancer>(offset);
  std::string error;
  auto result = conf_->set("rebalance_cb", rebalance_callback_.get(), error);
  if (result != RdKafka::Conf::ConfResult::CONF_OK) {
    return caf::make_error(ec::unspecified, "failed to set rebalance_cb: {}",
                           error);
  }
  return {};
}

configuration::rebalancer::rebalancer(int64_t offset) : offset_{offset} {
}

auto configuration::rebalancer::rebalance_cb(
  RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
  std::vector<RdKafka::TopicPartition*>& partitions) -> void {
  // This branching logic comes from the librdkafka consumer example. See the
  // implementation of ExampleRebalanceCb for details. The only thing we added
  // is the offset assignment at the beginning.
  if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
    if (offset_ != RdKafka::Topic::OFFSET_INVALID) {
      TENZIR_DEBUG("setting offset to {}", offset_);
      for (auto* partition : partitions) {
        partition->set_offset(offset_);
      }
    }
    if (consumer->rebalance_protocol() == "COOPERATIVE") {
      if (auto* err = consumer->incremental_assign(partitions)) {
        auto err_guard = std::unique_ptr<RdKafka::Error>{err};
        TENZIR_ERROR("failed to assign incrementally: {}", err_guard->str());
      };
    } else {
      auto err = consumer->assign(partitions);
      if (err != RdKafka::ERR_NO_ERROR) {
        TENZIR_ERROR("failed to assign partitions: {}", RdKafka::err2str(err));
      }
    }
  } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
    // Application may commit offsets manually here
    // if auto.commit.enable=false
    if (consumer->rebalance_protocol() == "COOPERATIVE") {
      if (auto* err = consumer->incremental_unassign(partitions)) {
        auto err_guard = std::unique_ptr<RdKafka::Error>{err};
        TENZIR_ERROR("failed to unassign incrementally: {}", err_guard->str());
      };
    } else {
      auto err = consumer->unassign();
      if (err != RdKafka::ERR_NO_ERROR) {
        TENZIR_ERROR("failed to unassign partitions: {}",
                     RdKafka::err2str(err));
      }
    }
  } else {
    TENZIR_ERROR("rebalancing error: {}", RdKafka::err2str(err));
    auto err = consumer->unassign();
    if (err != RdKafka::ERR_NO_ERROR) {
      TENZIR_ERROR("failed to unassign partitions: {}", RdKafka::err2str(err));
    }
  }
}

configuration::configuration() {
  conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  TENZIR_ASSERT(conf_, "RdKafka::Conf::create");
}

} // namespace tenzir::plugins::kafka
