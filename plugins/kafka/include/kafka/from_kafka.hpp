//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/box.hpp>
#include <tenzir/data.hpp>
#include <tenzir/location.hpp>
#include <tenzir/option.hpp>
#include <tenzir/startup_argument.hpp>
#include <tenzir/time.hpp>

#include <librdkafka/rdkafka.h>

#include <chrono>
#include <memory>
#include <string>

namespace tenzir::plugins::kafka {

/// Default delay before flushing a partial batch.
inline constexpr auto default_batch_timeout = std::chrono::milliseconds{100};

/// Default upper bound for one Kafka notification wait in the source poll loop.
inline constexpr auto default_fetch_wait_timeout = std::chrono::milliseconds{1};

/// Tells `from_kafka` where to read, for an operator that learns it only at
/// start, and keeps the consumer's credentials current after that.
class KafkaConnector {
public:
  struct Connection {
    std::string topic;
    /// librdkafka options. The `kafka.yaml` defaults do not apply, because
    /// they describe the user's own cluster.
    nova::Record options;
  };

  virtual ~KafkaConnector() = default;

  /// Emits a diagnostic if it fails.
  virtual auto connect(OpCtx& ctx) -> Task<failure_or<Connection>> = 0;

  /// Updates the credentials of `consumer` until the result is dropped, which
  /// has to happen before the consumer is destroyed.
  virtual auto watch(rd_kafka_t& consumer) -> std::shared_ptr<void> = 0;
};

/// Parsed arguments for `from_kafka`.
struct FromKafkaArgs {
  /// Kafka topic to consume.
  std::string topic;
  /// Maximum number of messages to emit before stopping.
  Option<located<uint64_t>> count;
  /// Stop after all assigned partitions reach EOF.
  Option<location> exit;
  /// Starting position to use for partition assignment.
  Option<located<data>> offset;
  /// Target number of messages per source batch.
  uint64_t batch_size = 10'000;
  /// Region used for MSK IAM token generation.
  Option<located<std::string>> aws_region;
  /// IAM authentication settings for OAUTHBEARER.
  Option<StartupArgument<nova::Record>> aws_iam;
  /// Raw librdkafka overrides applied to the consumer config.
  Option<StartupArgument<nova::Record>> options;
  /// Controls output ordering guarantees.
  std::string _optimization = "ordered";
  /// Preferred build-stage batch size override.
  uint64_t _worker_batch_size = 0;
  /// Capacity of the queue between source and build stages.
  uint64_t _prefetch_batches = 8;
  /// Byte budget for in-flight source payload.
  uint64_t _prefetch_bytes = uint64_t{256} << 20;
  /// Maximum wait before flushing a partial source batch.
  duration _batch_timeout = default_batch_timeout;
  /// Initial broker poll wait before adaptive timeout backoff.
  duration _fetch_wait_timeout = default_fetch_wait_timeout;
  Option<StartupArgument<nova::Secret>> schema_registry;
  Option<StartupArgument<nova::Record>> schema_registry_headers;
  Option<located<data>> schema_registry_tls;
  /// Whether the operator should treat the input as json and produce events
  /// directly
  bool _json = false;
  /// Whether to enable _raw mode for _json.
  bool _raw = false;
  // Set by operators built on this one rather than by users.
  /// Supplies `topic` and the connection options at start.
  std::shared_ptr<KafkaConnector> connector;
  /// The schema of the events of `_json`, if not `tenzir.unknown`.
  std::string schema_name;
  /// Restricts type inference for `_json` to this top-level field.
  std::string infer_under;
  /// The operator the read metrics are labeled with.
  std::string operator_name = "from_kafka";
  // An operator that turns the events into other events counts those instead.
  bool count_events = true;
  // Holds back the offsets of each delivered batch until the operator built on
  // this one releases the batch, rather than committing them right away.
  bool hold_offsets = false;
};

// The `from_kafka` that operators built on it drive.
class KafkaEventsSource : public Operator<void, nova::Events> {
public:
  // Returns how many batches `process_task` delivered with `hold_offsets`,
  // including those without events.
  virtual auto delivered_batches() const -> uint64_t = 0;

  // Commits the offsets of the first `count` delivered batches. Once the
  // stream ended and no batch is held anymore, it also leaves the group.
  virtual auto release_batches(uint64_t count, OpCtx& ctx) -> Task<void> = 0;
};

auto make_from_kafka_events(FromKafkaArgs args) -> Box<KafkaEventsSource>;

} // namespace tenzir::plugins::kafka
