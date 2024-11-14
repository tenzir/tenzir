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

#include <chrono>
#include <cstddef>
#include <memory>
#include <span>

#if TENZIR_ENABLE_BUNDLED_LIBRDKAFKA
#  include <rdkafkacpp.h>
#else
#  include <librdkafka/rdkafkacpp.h>
#endif

namespace tenzir::plugins::kafka {

class message;

/// Wraps a producer in a friendly interface.
class producer {
public:
  /// Constructs a producer from a configuration.
  static auto make(configuration config) -> caf::expected<producer>;

  /// Produces a message in the form of opaque bytes.
  auto produce(std::string topic, std::span<const std::byte> bytes,
               std::string_view key = {}, time timestamp = {}) -> caf::error;

  /// Polls the producer for events and invokes callbacks.
  auto poll(std::chrono::milliseconds timeout) -> int;

  /// Wait until all outstanding produce requests complete. This typically
  /// happens prior to destroying a producer instance to make sure all queued
  /// and in-flight produce requests are completed before terminating.
  /// @note This function calls poll internally.
  auto flush(std::chrono::milliseconds timeout) -> caf::error;

  /// Returns the length of the outbound queue that contains messages and
  /// requests waiting to be sent to or acknowledged by the broker.
  auto queue_size() const -> size_t;

private:
  producer() = default;

  configuration config_{};
  std::shared_ptr<RdKafka::Producer> producer_{};
};

} // namespace tenzir::plugins::kafka
