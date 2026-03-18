//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "kafka/operator.hpp"

#include <tenzir/pipeline.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <optional>
#include <string>

namespace tenzir::plugins::kafka::legacy {

// Legacy executor compatibility for `to_kafka`.
// Remove once the legacy executor path is deleted.
struct to_kafka_args {
  location op;
  std::string topic;
  ast::expression message = ast::function_call{
    ast::entity{{ast::identifier{"print_ndjson", location::unknown}}},
    {ast::this_{location::unknown}},
    location::unknown,
    true // method call
  };
  std::optional<located<std::string>> key;
  std::optional<located<time>> timestamp;
  located<record> options;
  std::optional<located<std::string>> aws_region;
  std::optional<tenzir::aws_iam_options> aws;
  uint64_t jobs = 0;

  friend auto inspect(auto& f, to_kafka_args& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("topic", x.topic),
                              f.field("message", x.message),
                              f.field("key", x.key),
                              f.field("timestamp", x.timestamp),
                              f.field("options", x.options),
                              f.field("aws_region", x.aws_region),
                              f.field("aws", x.aws), f.field("jobs", x.jobs));
  }
};

class to_kafka_operator final : public crtp_operator<to_kafka_operator> {
public:
  to_kafka_operator() = default;

  to_kafka_operator(to_kafka_args args, record config);

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate>;

  auto name() const -> std::string override;

  auto detached() const -> bool override;

  auto optimize(const expression&, event_order) const
    -> optimize_result override;

  friend auto inspect(auto& f, to_kafka_operator& x) -> bool {
    return f.object(x).fields(f.field("args_", x.args_),
                              f.field("config_", x.config_));
  }

private:
  to_kafka_args args_;
  record config_;
};

auto make_to_kafka(operator_factory_plugin::invocation inv, session ctx,
                   const record& defaults) -> failure_or<operator_ptr>;

} // namespace tenzir::plugins::kafka::legacy
