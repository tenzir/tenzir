//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"

#include "tenzir/detail/base64.hpp"
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
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>

#include <string_view>

namespace tenzir::plugins::kafka {

auto configuration::aws_iam_options::from_record(located<record> config,
                                                 diagnostic_handler& dh)
  -> failure_or<aws_iam_options> {
  constexpr auto known = std::array{
    "region",
    "assume_role",
    "session_name",
    "external_id",
  };
  const auto unknown = std::ranges::find_if(config.inner, [&](auto&& x) {
    return std::ranges::find(known, x.first) == std::ranges::end(known);
  });
  if (unknown != std::ranges::end(config.inner)) {
    diagnostic::error("unknown key '{}' in config", *unknown)
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  if (not config.inner.contains("region")) {
    diagnostic::error("'region' must be specified when using IAM")
      .primary(config)
      .emit(dh);
    return failure::promise();
  }
  const auto assign_non_empty_string
    = [&](std::string_view key, auto&& x) -> failure_or<void> {
    if (auto it = config.inner.find(key); it != config.inner.end()) {
      auto* extracted = try_as<std::string>(it->second.get_data());
      if (not extracted) {
        diagnostic::error("'{}' must be a `string`", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      if (extracted->empty()) {
        diagnostic::error("'{}' must not be empty", key)
          .primary(config)
          .emit(dh);
        return failure::promise();
      }
      x = std::move(*extracted);
    }
    return {};
  };
  auto opts = aws_iam_options{};
  opts.loc = config.source;
  TRY(assign_non_empty_string("region", opts.region));
  TRY(assign_non_empty_string("assume_role", opts.role));
  TRY(assign_non_empty_string("session_name", opts.session_name));
  TRY(assign_non_empty_string("external_id", opts.ext_id));
  return opts;
}

auto configuration::aws_iam_callback::oauthbearer_token_refresh_cb(
  RdKafka::Handle* handle, const std::string&) -> void {
  const auto valid_for = std::chrono::seconds{900};
  Aws::InitAPI({});
  const auto aws_guard = detail::scope_guard{[] noexcept {
    Aws::ShutdownAPI({});
  }};
  const auto url = Aws::Http::URI{
    fmt::format("https://kafka.{}.amazonaws.com/", options_.region),
  };
  auto request = Aws::Http::Standard::StandardHttpRequest{
    url,
    Aws::Http::HttpMethod::HTTP_GET,
  };
  auto provider = std::invoke(
    [&]() -> std::shared_ptr<Aws::Auth::AWSCredentialsProvider> {
      if (options_.role) {
        TENZIR_VERBOSE("[kafka iam] refreshing IAM Credentials for {}, {}, {}",
                       options_.region, options_.role.value(), valid_for);
        return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
          options_.role.value(),
          options_.session_name.value_or("tenzir-session"),
          options_.ext_id.value_or(""));
      }
      TENZIR_VERBOSE("[kafka iam] using the default credential chain");
      return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    });
  if (auto creds = provider->GetAWSCredentials(); creds.IsEmpty()) {
    diagnostic::warning("got empty AWS credentials")
      .primary(options_.loc)
      .emit(dh_);
  } else if (creds.IsExpired()) {
    diagnostic::warning("got expired AWS credentials")
      .primary(options_.loc)
      .emit(dh_);
  }
  request.AddQueryStringParameter("Action", "kafka-cluster:Connect");
  const auto signer = Aws::Client::AWSAuthV4Signer{
    provider,
    "kafka-cluster",
    options_.region,
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
  TENZIR_VERBOSE("[kafka iam] setting token");
  handle->oauthbearer_set_token(encoded, expiration.count(), "Tenzir", {},
                                errstr);
  if (not errstr.empty()) {
    diagnostic::error("failed to set oauth token: {}", errstr).emit(dh_);
  }
}

auto configuration::error_callback::event_cb(RdKafka::Event& event) -> void {
  const auto ty = event.type();
  if (ty == RdKafka::Event::EVENT_ERROR) {
    const auto* const severity = [&] {
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
    }();
    const auto error_code = event.err();
    const auto error_msg = event.str();
    diagnostic::warning("librdkafka {}: {} ({})", severity,
                        error_msg.empty() ? RdKafka::err2str(error_code)
                                          : error_msg,
                        std::to_underlying(error_code))
      .severity(event.fatal() ? severity::error : severity::warning)
      .emit(dh_);
  }
}

auto configuration::make(const record& options,
                         std::optional<aws_iam_options> aws,
                         diagnostic_handler& dh)
  -> caf::expected<configuration> {
  configuration result;
  if (auto err = result.set(options)) {
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
    TENZIR_VERBOSE("setting aws iam callback");
    result.aws_
      = std::make_shared<aws_iam_callback>(std::move(aws).value(), dh);
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
    if (auto err = set(key, tenzir::match(value, stringify))) {
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

configuration::rebalancer::rebalancer(int offset) : offset_{offset} {
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
        TENZIR_ERROR("failed to assign incrementally: {}", err->str());
        delete err;
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
        TENZIR_ERROR("failed to unassign incrementally: {}", err->str());
        delete err;
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
