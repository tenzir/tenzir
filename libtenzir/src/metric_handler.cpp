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

metric_handler::metric_handler(metrics_receiver_actor new_receiver,
                               uint64_t operator_index, uint64_t metric_index,
                               type metric_type)
  : receiver_{new_receiver},
    op_index_{operator_index},
    metric_index_{metric_index} {
  caf::anon_send(new_receiver, op_index_, metric_index_,
                 type{metric_type, {{"internal", ""}}});
}

auto metric_handler::emit(record&& r) -> void {
  if (auto receiver = receiver_.lock()) {
    // Explicitly create a data-from-time cast here to support macOS builds.
    r["timestamp"] = data{time{time::clock::now()}};
    r["operator_id"] = op_index_;
    caf::anon_send(receiver_.lock(), op_index_, metric_index_, std::move(r));
  }
}

} // namespace tenzir