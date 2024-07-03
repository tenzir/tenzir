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
  uint64_t operator_index)
  : receiver{std::move(new_receiver)}, op_index{operator_index} {
}

auto metric_handler::emit(const std::string& schema, record&& r) -> void {
  // Explicitly create a data-from-time cast here to support macOS builds.
  r["timestamp"] = data{time{time::clock::now()}};
  r["operator_index"] = op_index;
  caf::anon_send(receiver.lock(), schema, std::move(r));
}

auto metric_handler::emit(operator_metric&& m) -> void {
  caf::anon_send(receiver.lock(), std::move(m));
}

} // namespace tenzir