//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "kafka/configuration.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace tenzir::plugins::kafka {

/// Wraps a `RdKafka::Consumer` in a friendly interface.
class consumer {
public:
  /// Constructs a consumer from a configuration.
  static auto make(configuration config) -> caf::expected<consumer>;

  /// Subscribes to a list of topics.
  auto subscribe(const std::vector<std::string>& topics) -> caf::error;

  /// Consumes a raw message, blocking for a given maximum timeout.
  auto consume_raw(std::chrono::milliseconds timeout)
    -> std::shared_ptr<RdKafka::Message>;

  /// Commits offset for a specific message synchronously.
  auto commit(RdKafka::Message* message) -> caf::error;

  /// Commits offset for a specific message asynchronously.
  auto commit_async(RdKafka::Message* message) -> caf::error;

  ~consumer() {
    if (consumer_.use_count() == 1) {
      consumer_->close();
    }
  }

private:
  consumer() = default;

  configuration config_{};
  std::shared_ptr<RdKafka::KafkaConsumer> consumer_{};
};

} // namespace tenzir::plugins::kafka
