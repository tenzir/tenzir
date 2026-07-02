//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/atomic.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution.hpp>

#include <caf/expected.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
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

/// Owns librdkafka consumer config and callback objects with shared lifetime.
struct consumer_configuration {
  std::shared_ptr<RdKafka::Conf> conf;
  std::shared_ptr<RdKafka::OAuthBearerTokenRefreshCb> oauth_callback;
  std::shared_ptr<RdKafka::EventCb> event_callback;
  std::shared_ptr<RdKafka::RebalanceCb> rebalance_callback;
  std::shared_ptr<Atomic<uint64_t>> assignment_generation;
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

/// One recorded producer message-delivery failure.
struct kafka_delivery_failure {
  RdKafka::ErrorCode error_code = RdKafka::ERR_NO_ERROR;
  std::string error_string;
  std::string topic;
  size_t message_size = 0;
};

/// Thread-safe collector for librdkafka producer delivery-report failures.
///
/// librdkafka invokes the delivery-report callback once per produced message
/// from whichever thread services `poll()`/`flush()`, and producers created
/// from a copied configuration share one instance, so all access is
/// synchronized. Only the first failure is retained; `take()` returns it
/// exactly once so a single diagnostic is emitted no matter how many workers
/// observe it.
class kafka_delivery_report_state {
public:
  /// Records the outcome of one delivery report. Successful deliveries and
  /// failures after the first are ignored.
  auto record(RdKafka::Message const& message) -> void {
    if (message.err() == RdKafka::ERR_NO_ERROR) {
      return;
    }
    auto guard = std::scoped_lock{mutex_};
    if (failure_) {
      return;
    }
    failure_ = kafka_delivery_failure{
      message.err(),
      RdKafka::err2str(message.err()),
      message.topic_name(),
      message.len(),
    };
  }

  /// Returns the first recorded failure exactly once, then nothing.
  auto take() -> std::optional<kafka_delivery_failure> {
    auto guard = std::scoped_lock{mutex_};
    if (not failure_ or reported_) {
      return std::nullopt;
    }
    reported_ = true;
    return failure_;
  }

private:
  mutable std::mutex mutex_;
  bool reported_ = false;
  std::optional<kafka_delivery_failure> failure_;
};

/// Owns librdkafka producer config and callback objects with shared lifetime.
struct producer_configuration {
  std::shared_ptr<RdKafka::Conf> conf;
  std::shared_ptr<RdKafka::OAuthBearerTokenRefreshCb> oauth_callback;
  std::shared_ptr<RdKafka::EventCb> event_callback;
  std::shared_ptr<RdKafka::DeliveryReportCb> delivery_callback;
  std::shared_ptr<kafka_delivery_report_state> delivery_state;
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
  diagnostic_builder out,
  std::optional<resolved_aws_credentials> const& credentials)
  -> diagnostic_builder;

/// Creates a consumer configuration from static options plus callback setup.
auto make_consumer_configuration(record const& options,
                                 std::optional<aws_iam_options> aws,
                                 std::optional<resolved_aws_credentials> creds,
                                 int64_t offset, diagnostic_handler& dh)
  -> caf::expected<consumer_configuration>;

/// Creates a producer configuration from static options plus callback setup.
auto make_producer_configuration(record const& options,
                                 std::optional<aws_iam_options> aws,
                                 std::optional<resolved_aws_credentials> creds,
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
