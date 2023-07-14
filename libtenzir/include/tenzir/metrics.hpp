//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <caf/timespan.hpp>
#include <caf/timestamp.hpp>

namespace tenzir {

struct [[nodiscard]] pipeline_op_metrics {
  // Metrics that track the total number of inbound and outbound elements that
  // passed through this operator.
  std::chrono::steady_clock::time_point start_time
    = std::chrono::steady_clock::now();
  caf::timespan time_starting = {};
  caf::timespan time_running = {};
  caf::timespan time_scheduled = {};
  uint64_t inbound_total = {};
  uint64_t num_inbound_batches = {};
  uint64_t outbound_total = {};
  uint64_t num_outbound_batches = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, pipeline_op_metrics& x) -> bool {
    return f.object(x)
      .pretty_name("pipeline_op_metrics")
      .fields( // f.field("start_time", x.start_time),
        f.field("time_starting", x.time_starting),
        f.field("time_running", x.time_running),
        f.field("time_scheduled", x.time_scheduled),
        f.field("inbound_total", x.inbound_total),
        f.field("num_inbound_batches", x.num_inbound_batches),
        f.field("outbound_total", x.outbound_total),
        f.field("num_outbound_batches", x.num_outbound_batches));
  }
};

struct [[nodiscard]] pipeline_total_metrics {
  // Metrics that track the total number of inbound and outbound elements that
  // passed through this operator.
  std::chrono::steady_clock::time_point start_time = {};
  std::chrono::steady_clock::time_point end_time = {};
  uint64_t inbound_total = {};
  double inbound_total_rate = {};
  uint64_t outbound_total = {};
  double outbound_total_rate = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, pipeline_total_metrics& x) -> bool {
    return f.object(x)
      .pretty_name("pipeline_total_metrics")
      .fields( // f.field("start_time", x.start_time),
               // f.field("time_running", x.end_time),
        f.field("inbound_total", x.inbound_total),
        f.field("inbound_total_rate", x.inbound_total_rate),
        f.field("outbound_total", x.outbound_total),
        f.field("outbound_total_rate", x.outbound_total_rate));
  }
};

} // namespace tenzir
