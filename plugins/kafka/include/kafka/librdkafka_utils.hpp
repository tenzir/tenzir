//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <tenzir/atomic.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution.hpp>

#include <caf/expected.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::kafka {

/// Tracks partitions whose offsets the current consumer run has committed.
///
/// The operator's commit path writes from executor tasks while librdkafka's
/// rebalance callback reads from whichever thread serves the consumer queue.
/// The callback cannot await a coroutine mutex, so a plain lock guards the
/// tiny critical sections instead.
class committed_partition_set {
public:
  auto insert(std::string topic, int32_t partition) -> void {
    auto guard = std::scoped_lock{mutex_};
    partitions_.emplace(std::move(topic), partition);
  }

  auto contains(std::string const& topic, int32_t partition) const -> bool {
    auto guard = std::scoped_lock{mutex_};
    return partitions_.contains({topic, partition});
  }

private:
  mutable std::mutex mutex_;
  std::set<std::pair<std::string, int32_t>> partitions_;
};

/// The kind of the rebalance event that produced the current assignment.
///
/// The distinction matters because the eager rebalance protocol revokes every
/// partition at once: between the revoke and the following assign, the
/// consumer's assignment is empty even though the run is not over. Only an
/// assign reports an assignment that may be acted upon.
enum class AssignmentChange : uint8_t {
  /// The broker assigned a — possibly empty — set of partitions.
  assigned,
  /// The broker revoked partitions and has not assigned the new set yet.
  revoked,
};

/// Owns librdkafka consumer config and callback objects with shared lifetime.
struct consumer_configuration {
  std::shared_ptr<RdKafka::Conf> conf;
  std::shared_ptr<RdKafka::OAuthBearerTokenRefreshCb> oauth_callback;
  std::shared_ptr<RdKafka::EventCb> event_callback;
  std::shared_ptr<RdKafka::RebalanceCb> rebalance_callback;
  std::shared_ptr<Atomic<uint64_t>> assignment_generation;
  /// Serializes assignment changes with commits tied to an assignment
  /// generation.
  std::shared_ptr<std::mutex> assignment_mutex;
  /// The kind of the event that produced the current generation.
  ///
  /// The rebalance callback writes this before bumping `assignment_generation`
  /// and readers load it after observing a new generation, so the generation's
  /// release/acquire pair publishes this value too. librdkafka invokes the
  /// rebalance callback from one thread at a time, so the plain store needs no
  /// further ordering. Two rebalances between two reads collapse into the most
  /// recent kind, which is the one a reader wants.
  std::shared_ptr<Atomic<AssignmentChange>> last_assignment_change;
  std::shared_ptr<committed_partition_set> committed_partitions;
  // `enable_sasl_queue(true)` is configured on `Conf` before consumer
  // creation. This is required to later attach OAUTH callback servicing to
  // librdkafka's background thread.
  bool oauth_sasl_queue_enabled = false;
  // Set to true only after `sasl_background_callbacks_enable()` succeeds on
  // the created consumer handle.
  bool oauth_background_callbacks_active = false;
  // Human-readable setup detail used by metadata diagnostics when we fall back
  // to poll-driven callback servicing.
  std::string oauth_background_setup_note;
};

/// Owns librdkafka producer config and callback objects with shared lifetime.
struct producer_configuration {
  std::shared_ptr<RdKafka::Conf> conf;
  std::shared_ptr<RdKafka::OAuthBearerTokenRefreshCb> oauth_callback;
  std::shared_ptr<RdKafka::EventCb> event_callback;
};

/// Returns one librdkafka config value or a diagnostic placeholder.
auto kafka_conf_value(RdKafka::Conf const* conf, std::string_view key)
  -> std::string;

/// Adds common connection-related Kafka config notes to one diagnostic.
auto add_kafka_connection_diagnostic_notes(diagnostic_builder out,
                                           RdKafka::Conf const* conf)
  -> diagnostic_builder;

/// Adds AWS IAM mode/region/profile/role notes when credentials are available.
auto add_kafka_aws_iam_diagnostic_notes(
  diagnostic_builder out, Option<resolved_aws_credentials> const& credentials)
  -> diagnostic_builder;

/// Creates a consumer configuration from static options plus callback setup.
auto make_consumer_configuration(record const& options,
                                 Option<aws_iam_options> aws,
                                 Option<resolved_aws_credentials> creds,
                                 int64_t offset, diagnostic_handler& dh)
  -> caf::expected<consumer_configuration>;

/// Creates a producer configuration from static options plus callback setup.
auto make_producer_configuration(record const& options,
                                 Option<aws_iam_options> aws,
                                 Option<resolved_aws_credentials> creds,
                                 diagnostic_handler& dh)
  -> caf::expected<producer_configuration>;

/// Applies plain options and returns secret requests for deferred resolution.
[[nodiscard]] auto
configure_consumer_or_request_secrets(consumer_configuration& cfg,
                                      located<record> const& options,
                                      diagnostic_handler& dh)
  -> std::vector<secret_request>;

/// Applies plain options and returns secret requests for deferred resolution.
[[nodiscard]] auto
configure_producer_or_request_secrets(producer_configuration& cfg,
                                      located<record> const& options,
                                      diagnostic_handler& dh)
  -> std::vector<secret_request>;

} // namespace tenzir::plugins::kafka
