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
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution.hpp>

#include <caf/expected.hpp>
#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace tenzir::plugins::kafka {

/// Owns librdkafka consumer config and callback objects with shared lifetime.
struct consumer_configuration {
  std::shared_ptr<RdKafka::Conf> conf;
  std::shared_ptr<RdKafka::OAuthBearerTokenRefreshCb> oauth_callback;
  std::shared_ptr<RdKafka::EventCb> event_callback;
  std::shared_ptr<RdKafka::RebalanceCb> rebalance_callback;
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
