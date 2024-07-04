//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/metric_handler.hpp"

#include "tenzir/pipeline.hpp"

#include <caf/send.hpp>

namespace tenzir {

metric_handler::metric_handler(
  detail::weak_handle<metrics_receiver_actor> new_receiver,
  uint64_t operator_index, type metric_type)
  : receiver_{std::move(new_receiver)}, op_index_{operator_index} {
  const auto* metric_record_type = caf::get_if<record_type>(&metric_type);
  TENZIR_ASSERT(metric_record_type);
  if (metric_record_type->num_fields() == 0) {
    return;
  }
  auto params_type = metric_record_type->transform({{
    {metric_record_type->num_fields() - 1},
    record_type::insert_after({
      {"timestamp", time_type{}},
      {"operator_id", uint64_type{}},
    }),
  }});
  metric_type_ = type{metric_type.name(), *params_type};
}

auto metric_handler::emit(record&& r) -> void {
  // Explicitly create a data-from-time cast here to support macOS builds.
  r["timestamp"] = data{time{time::clock::now()}};
  r["operator_index"] = op_index;
  caf::anon_send(receiver_.lock(), metric_type_, std::move(r));
}

auto metric_handler::emit(operator_metric&& m) -> void {
  caf::anon_send(receiver_.lock(), std::move(m));
}

} // namespace tenzir