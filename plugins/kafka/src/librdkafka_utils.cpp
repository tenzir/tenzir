//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/librdkafka_utils.hpp"

#include "tenzir/detail/base64.hpp"

#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/error.hpp>
#include <tenzir/time.hpp>

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

#include <chrono>
#include <string>
#include <string_view>

namespace tenzir::plugins::kafka {

namespace {

/// Callback that refreshes Kafka OAUTHBEARER tokens via AWS IAM signing.
class aws_iam_refresh_callback final
  : public RdKafka::OAuthBearerTokenRefreshCb {
public:
  /// Constructs a callback with resolved IAM option inputs.
  aws_iam_refresh_callback(aws_iam_options options,
                           std::optional<resolved_aws_credentials> creds,
                           diagnostic_handler& dh)
    : options_{std::move(options)}, creds_{std::move(creds)}, dh_{dh} {
  }

  /// Produces one signed token and installs it on the librdkafka handle.
  auto oauthbearer_token_refresh_cb(RdKafka::Handle* handle, std::string const&)
    -> void override {
    TENZIR_DEBUG("[from_kafka] refreshing OAUTHBEARER token via AWS IAM");
    constexpr auto valid_for = std::chrono::seconds{900};
    Aws::InitAPI({});
    auto aws_guard = detail::scope_guard{[] noexcept {
      Aws::ShutdownAPI({});
    }};

    TENZIR_ASSERT(creds_ and not creds_->region.empty());
    auto const& region = creds_->region;
    auto url
      = Aws::Http::URI{fmt::format("https://kafka.{}.amazonaws.com/", region)};
    auto request = Aws::Http::Standard::StandardHttpRequest{
      url,
      Aws::Http::HttpMethod::HTTP_GET,
    };

    auto provider
      = [&]() -> std::shared_ptr<Aws::Auth::AWSCredentialsProvider> {
      auto base_provider = std::shared_ptr<Aws::Auth::AWSCredentialsProvider>{};
      if (creds_ and not creds_->access_key_id.empty()) {
        TENZIR_DEBUG("[from_kafka] using explicit AWS credentials");
        base_provider
          = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
            creds_->access_key_id, creds_->secret_access_key,
            creds_->session_token);
      } else if (creds_ and not creds_->profile.empty()) {
        TENZIR_DEBUG("[from_kafka] using AWS profile `{}`", creds_->profile);
        base_provider
          = std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
            creds_->profile.c_str());
      } else {
        TENZIR_DEBUG("[from_kafka] using default AWS credentials chain");
        base_provider
          = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
      }
      if (creds_ and not creds_->role.empty()) {
        TENZIR_DEBUG("[from_kafka] assuming IAM role `{}`", creds_->role);
        auto sts_config = Aws::Client::ClientConfiguration{};
        sts_config.region = region;
        auto sts_client = std::make_shared<Aws::STS::STSClient>(
          base_provider, nullptr, sts_config);
        auto session_name = creds_->session_name.empty()
                              ? std::string{"tenzir-session"}
                              : creds_->session_name;
        return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
          creds_->role, session_name, creds_->external_id,
          Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts_client);
      }
      return base_provider;
    }();

    if (auto creds = provider->GetAWSCredentials(); creds.IsEmpty()) {
      diagnostic::warning("got empty AWS credentials")
        .primary(options_.loc)
        .emit(dh_);
    } else if (creds.IsExpired()) {
      diagnostic::warning("got expired AWS credentials")
        .primary(options_.loc)
        .emit(dh_);
    } else {
      TENZIR_DEBUG("[from_kafka] AWS credentials resolved successfully");
    }

    request.AddQueryStringParameter("Action", "kafka-cluster:Connect");
    auto signer = Aws::Client::AWSAuthV4Signer{
      provider,
      "kafka-cluster",
      region,
    };
    TENZIR_ASSERT(signer.PresignRequest(request, valid_for.count()));
    request.AddQueryStringParameter("User-Agent", "Tenzir");

    auto encoded = detail::base64::encode(request.GetURIString());
    boost::trim_right_if(encoded, [](char c) {
      return c == '=';
    });
    boost::replace_all(encoded, "+", "-");
    boost::replace_all(encoded, "/", "_");

    auto errstr = std::string{};
    auto expiration = duration_cast<std::chrono::milliseconds>(
      (time::clock::now() + valid_for).time_since_epoch());
    handle->oauthbearer_set_token(encoded, expiration.count(), "Tenzir", {},
                                  errstr);
    if (not errstr.empty()) {
      diagnostic::error("failed to set oauth token: {}", errstr).emit(dh_);
    } else {
      TENZIR_DEBUG("[from_kafka] OAUTHBEARER token set successfully, expires "
                   "in {} seconds",
                   valid_for.count());
    }
  }

private:
  aws_iam_options options_;
  std::optional<resolved_aws_credentials> creds_;
  diagnostic_handler& dh_;
};

/// Callback that forwards librdkafka warnings/errors to diagnostics.
class error_event_callback final : public RdKafka::EventCb {
public:
  /// Constructs an event callback bound to one diagnostic sink.
  explicit error_event_callback(diagnostic_handler& dh) : dh_{dh} {
  }

  /// Reports warning/error events while preserving librdkafka severity.
  auto event_cb(RdKafka::Event& event) -> void override {
    auto severity_name = [&]() -> const char* {
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
    auto error_code = event.err();
    auto error_msg = event.str();
    if (event.type() == RdKafka::Event::EVENT_ERROR
        or event.severity() >= RdKafka::Event::EVENT_SEVERITY_WARNING) {
      diagnostic::warning("librdkafka {}: {} ({})", severity_name(),
                          error_msg.empty() ? RdKafka::err2str(error_code)
                                            : error_msg,
                          std::to_underlying(error_code))
        .severity(event.fatal() ? severity::error : severity::warning)
        .emit(dh_);
    }
  }

private:
  diagnostic_handler& dh_;
};

/// Callback that assigns explicit offsets during partition rebalance.
class rebalance_callback final : public RdKafka::RebalanceCb {
public:
  /// Constructs a callback that assigns `offset` for new partitions.
  explicit rebalance_callback(int64_t offset) : offset_{offset} {
  }

  /// Handles assign/revoke events while preserving cooperative semantics.
  auto rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition*>& partitions)
    -> void override {
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      if (offset_ != RdKafka::Topic::OFFSET_INVALID) {
        for (auto* partition : partitions) {
          partition->set_offset(offset_);
        }
      }
      if (consumer->rebalance_protocol() == "COOPERATIVE") {
        if (auto* e = consumer->incremental_assign(partitions)) {
          auto err_guard = std::unique_ptr<RdKafka::Error>{e};
          TENZIR_ERROR("failed to assign incrementally: {}", err_guard->str());
        }
      } else {
        if (auto assign_err = consumer->assign(partitions);
            assign_err != RdKafka::ERR_NO_ERROR) {
          TENZIR_ERROR("failed to assign partitions: {}",
                       RdKafka::err2str(assign_err));
        }
      }
      return;
    }
    if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
      if (consumer->rebalance_protocol() == "COOPERATIVE") {
        if (auto* e = consumer->incremental_unassign(partitions)) {
          auto err_guard = std::unique_ptr<RdKafka::Error>{e};
          TENZIR_ERROR("failed to unassign incrementally: {}",
                       err_guard->str());
        }
      } else if (auto unassign_err = consumer->unassign();
                 unassign_err != RdKafka::ERR_NO_ERROR) {
        TENZIR_ERROR("failed to unassign partitions: {}",
                     RdKafka::err2str(unassign_err));
      }
      return;
    }
    TENZIR_ERROR("rebalancing error: {}", RdKafka::err2str(err));
    if (auto unassign_err = consumer->unassign();
        unassign_err != RdKafka::ERR_NO_ERROR) {
      TENZIR_ERROR("failed to unassign partitions: {}",
                   RdKafka::err2str(unassign_err));
    }
  }

private:
  int64_t offset_ = RdKafka::Topic::OFFSET_INVALID;
};

/// Converts one librdkafka conf set result into a typed `caf::error`.
auto set_conf_option(RdKafka::Conf& conf, std::string const& key,
                     std::string const& value) -> caf::error {
  auto error = std::string{};
  auto result = conf.set(key, value, error);
  switch (result) {
    case RdKafka::Conf::ConfResult::CONF_UNKNOWN:
      return caf::make_error(ec::unspecified,
                             fmt::format("unknown configuration property: {}",
                                         error));
    case RdKafka::Conf::ConfResult::CONF_INVALID:
      return caf::make_error(
        ec::unspecified, fmt::format("invalid configuration value: {}", error));
    case RdKafka::Conf::ConfResult::CONF_OK:
      return {};
  }
  TENZIR_UNREACHABLE();
}

/// Applies one typed Tenzir record to a librdkafka conf object.
auto apply_record_options(RdKafka::Conf& conf, record const& options)
  -> caf::error {
  auto stringify = detail::overload{
    [](std::string const& value) {
      return value;
    },
    [](auto const& value) {
      return to_string(value);
    },
  };
  for (auto const& [key, value] : options) {
    if (auto err = set_conf_option(conf, key, match(value, stringify)); err) {
      return err;
    }
  }
  return {};
}

/// Sets one callback pointer on a librdkafka conf object.
template <class Callback>
auto set_conf_callback(RdKafka::Conf& conf, std::string_view key,
                       Callback* callback) -> caf::error {
  auto errstr = std::string{};
  auto result = conf.set(std::string{key}, callback, errstr);
  if (result != RdKafka::Conf::ConfResult::CONF_OK) {
    return caf::make_error(ec::unspecified,
                           fmt::format("failed to set {}: {}", key, errstr));
  }
  return {};
}

/// Sets one typed option and emits a diagnostic on failure.
auto set_conf_or_emit(RdKafka::Conf& conf, std::string const& key,
                      std::string const& value, location loc,
                      diagnostic_handler& dh) -> void {
  if (auto err = set_conf_option(conf, key, value); err) {
    diagnostic::error("failed to set librdkafka option {}={}: {}", key, value,
                      err)
      .primary(loc)
      .emit(dh);
  }
}

} // namespace

auto make_consumer_configuration(record const& options,
                                 std::optional<aws_iam_options> aws,
                                 std::optional<resolved_aws_credentials> creds,
                                 int64_t offset, diagnostic_handler& dh)
  -> caf::expected<consumer_configuration> {
  auto cfg = consumer_configuration{};
  cfg.conf = std::shared_ptr<RdKafka::Conf>{
    RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL),
  };
  TENZIR_ASSERT(cfg.conf, "RdKafka::Conf::create");

  if (auto err = apply_record_options(*cfg.conf, options); err) {
    return err;
  }

  cfg.event_callback = std::make_shared<error_event_callback>(dh);
  if (auto err
      = set_conf_callback(*cfg.conf, "event_cb", cfg.event_callback.get());
      err) {
    return err;
  }

  cfg.rebalance_callback = std::make_shared<rebalance_callback>(offset);
  if (auto err = set_conf_callback(*cfg.conf, "rebalance_cb",
                                   cfg.rebalance_callback.get());
      err) {
    return err;
  }

  if (aws) {
    cfg.oauth_callback = std::make_shared<aws_iam_refresh_callback>(
      std::move(*aws), std::move(creds), dh);
    if (auto err = set_conf_callback(*cfg.conf, "oauthbearer_token_refresh_cb",
                                     cfg.oauth_callback.get());
        err) {
      return err;
    }
  }

  return cfg;
}

[[nodiscard]] auto
configure_consumer_or_request_secrets(consumer_configuration& cfg,
                                      located<record> const& options,
                                      diagnostic_handler& dh)
  -> std::vector<secret_request> {
  TENZIR_ASSERT(cfg.conf);
  auto requests = std::vector<secret_request>{};
  for (auto const& [key, value] : options.inner) {
    match(
      value,
      [&](concepts::arithmetic auto const& v) {
        set_conf_or_emit(*cfg.conf, key, fmt::to_string(v), options.source, dh);
      },
      [&](std::string const& s) {
        set_conf_or_emit(*cfg.conf, key, s, options.source, dh);
      },
      [&](secret const& s) {
        requests.emplace_back(
          s, options.source,
          [&cfg, &dh, key, loc = options.source](
            resolved_secret_value const& v) -> failure_or<void> {
            TRY(auto str, v.utf8_view("options." + key, loc, dh));
            set_conf_or_emit(*cfg.conf, key, std::string{str}, loc, dh);
            return {};
          });
      },
      [](auto const&) {
        TENZIR_UNREACHABLE();
      });
  }
  return requests;
}

} // namespace tenzir::plugins::kafka
