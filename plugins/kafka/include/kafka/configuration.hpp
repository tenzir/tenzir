//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/diagnostics.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/location.hpp>
#include <tenzir/type.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>
#include <string_view>

namespace tenzir::plugins::kafka {

/// Wraps a global Kafka configuration.
class configuration {
  friend class producer;
  friend class consumer;

public:
  struct aws_iam_options {
    std::string region;
    std::optional<std::string> role;
    std::optional<std::string> session_name;
    std::optional<std::string> ext_id;
    location loc;

    static auto from_record(located<record> config, diagnostic_handler& dh)
      -> failure_or<aws_iam_options>;
  };

  class aws_iam_callback : public RdKafka::OAuthBearerTokenRefreshCb {
  public:
    aws_iam_callback(aws_iam_options options, diagnostic_handler& dh)
      : options_{std::move(options)}, dh_{dh} {
    }

    auto oauthbearer_token_refresh_cb(RdKafka::Handle*, const std::string&)
      -> void override;

  private:
    aws_iam_options options_;
    diagnostic_handler& dh_;
  };

  /// Creates a configuration from a record.
  static auto make(const record& options, std::optional<aws_iam_options> aws,
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

  /// Sets a rebalance callback.
  auto set_rebalance_cb(int64_t offset) -> caf::error;

private:
  class rebalancer : public RdKafka::RebalanceCb {
  public:
    explicit rebalancer(int offset);

    auto rebalance_cb(RdKafka::KafkaConsumer*, RdKafka::ErrorCode,
                      std::vector<RdKafka::TopicPartition*>&) -> void override;

  private:
    int64_t offset_ = RdKafka::Topic::OFFSET_INVALID;
  };

  configuration();

  std::shared_ptr<RdKafka::Conf> conf_{};
  std::shared_ptr<aws_iam_callback> aws_{};
  std::shared_ptr<rebalancer> rebalance_callback_;
};

} // namespace tenzir::plugins::kafka
