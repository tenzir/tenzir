//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/aws_iam.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/location.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/type.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

namespace tenzir::plugins::kafka {

/// Wraps a global Kafka configuration.
class configuration {
  friend class producer;
  friend class consumer;

public:
  class aws_iam_callback : public RdKafka::OAuthBearerTokenRefreshCb {
  public:
    aws_iam_callback(tenzir::aws_iam_options options,
                     std::optional<tenzir::resolved_aws_credentials> creds,
                     diagnostic_handler& dh)
      : options_{std::move(options)}, creds_{std::move(creds)}, dh_{dh} {
    }

    auto oauthbearer_token_refresh_cb(RdKafka::Handle*, const std::string&)
      -> void override;

  private:
    tenzir::aws_iam_options options_;
    std::optional<tenzir::resolved_aws_credentials> creds_;
    diagnostic_handler& dh_;
  };

  class error_callback : public RdKafka::EventCb {
  public:
    explicit error_callback(diagnostic_handler& dh) : dh_{dh} {
    }

    auto event_cb(RdKafka::Event& event) -> void override;

  private:
    diagnostic_handler& dh_;
  };

  /// Records producer delivery-report failures so that asynchronous drops
  /// (e.g. a broker-side "message too large") surface as diagnostics instead
  /// of silent data loss. librdkafka invokes `dr_cb` from whichever thread
  /// services `poll()`/`flush()`, and producers created from a copied
  /// configuration share one instance, hence the synchronization.
  class delivery_callback : public RdKafka::DeliveryReportCb {
  public:
    auto dr_cb(RdKafka::Message& message) -> void override;

    /// Returns the first recorded delivery failure as an error exactly once.
    auto take_error() -> caf::error;

  private:
    std::mutex mutex_;
    bool failed_ = false;
    bool reported_ = false;
    RdKafka::ErrorCode code_ = RdKafka::ERR_NO_ERROR;
    std::string topic_;
    size_t size_ = 0;
  };

  /// Creates a configuration from a record.
  static auto
  make(const record& options, std::optional<tenzir::aws_iam_options> aws,
       std::optional<tenzir::resolved_aws_credentials> creds,
       diagnostic_handler& dh) -> caf::expected<configuration>;

  /// Gets a value for a given key.
  auto get(std::string_view key) const -> caf::expected<std::string>;

  /// Sets a value for a given key.
  auto set(const std::string& key, const std::string& value) -> caf::error;

  /// Sets key-value pairs based on a record.
  /// @note Even though the documentation mentions specific config value types,
  /// the API of librdkafka itself only operates on strings. As a result, this
  /// function translates all typed values in strings prior to passing them to
  /// librdkafa.
  auto set(const record& options) -> caf::error;

  /// Registers a delivery-report callback so that producer delivery failures
  /// are captured. Must be called once, before any producer is created from
  /// this configuration. Only meaningful for producers.
  auto enable_delivery_reports() -> caf::error;

  /// Returns the first producer delivery failure recorded by the
  /// delivery-report callback as an error, exactly once. Producers created
  /// from a copy of this configuration share the same callback, so this
  /// observes failures from all of them. Returns a default-constructed error
  /// if none occurred or if delivery reports were not enabled.
  auto take_delivery_error() -> caf::error;

  /// Sets a rebalance callback.
  auto set_rebalance_cb(int64_t offset) -> caf::error;

  /// Returns the underlying librdkafka configuration handle.
  auto underlying() const -> RdKafka::Conf* {
    return conf_.get();
  }

private:
  class rebalancer : public RdKafka::RebalanceCb {
  public:
    explicit rebalancer(int64_t offset);

    auto rebalance_cb(RdKafka::KafkaConsumer*, RdKafka::ErrorCode,
                      std::vector<RdKafka::TopicPartition*>&) -> void override;

  private:
    int64_t offset_ = RdKafka::Topic::OFFSET_INVALID;
  };

  configuration();

  std::shared_ptr<RdKafka::Conf> conf_;
  std::shared_ptr<aws_iam_callback> aws_;
  std::shared_ptr<rebalancer> rebalance_callback_;
  std::shared_ptr<error_callback> error_callback_;
  std::shared_ptr<delivery_callback> delivery_callback_;
};

} // namespace tenzir::plugins::kafka
