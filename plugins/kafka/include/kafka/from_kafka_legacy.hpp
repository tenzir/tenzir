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
#include <unordered_map>
#include <unordered_set>

namespace tenzir::plugins::kafka::legacy {

// Legacy executor compatibility for `from_kafka`.
// Remove once the legacy executor path is deleted.
struct from_kafka_args {
  std::string topic;
  std::optional<located<uint64_t>> count;
  std::optional<location> exit;
  std::optional<located<std::string>> offset;
  std::uint64_t commit_batch_size = 1000;
  located<record> options;
  std::optional<located<std::string>> aws_region;
  std::optional<tenzir::aws_iam_options> aws;
  location operator_location;
  uint64_t jobs = 1;

  friend auto inspect(auto& f, from_kafka_args& x) -> bool {
    return f.object(x).fields(
      f.field("topic", x.topic), f.field("count", x.count),
      f.field("exit", x.exit), f.field("offset", x.offset),
      f.field("commit_batch_size", x.commit_batch_size),
      f.field("options", x.options), f.field("aws_region", x.aws_region),
      f.field("aws", x.aws), f.field("operator_location", x.operator_location),
      f.field("jobs", x.jobs));
  }
};

class from_kafka_operator final : public crtp_operator<from_kafka_operator> {
public:
  from_kafka_operator() = default;

  from_kafka_operator(from_kafka_args args, record config);

  auto operator()(operator_control_plane& ctrl) const -> generator<table_slice>;

  auto detached() const -> bool override;

  auto optimize(const expression&, event_order) const
    -> optimize_result override;

  auto name() const -> std::string override;

  friend auto inspect(auto& f, from_kafka_operator& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("config", x.config_));
  }

private:
  from_kafka_args args_;
  record config_;
};

auto make_from_kafka(operator_factory_invocation inv, session ctx,
                     const record& defaults) -> failure_or<operator_ptr>;

} // namespace tenzir::plugins::kafka::legacy
