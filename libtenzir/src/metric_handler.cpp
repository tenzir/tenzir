//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/metric_handler.hpp"

#include <caf/send.hpp>

namespace tenzir {

metric_handler::metric_handler(metrics_receiver_actor receiver,
                               uint64_t operator_index, uint64_t metric_index,
                               const type& metric_type)
  : receiver_{std::move(receiver)},
    op_index_{operator_index},
    metric_index_{metric_index} {
  caf::anon_send(receiver_, op_index_, metric_index_,
                 type{metric_type, {{"internal", ""}}});
}

auto metric_handler::emit(record&& r) -> void {
  // Explicitly create a data-from-time cast here to support macOS builds.
  r.emplace("timestamp", time{time::clock::now()});
  r.emplace("operator_id", op_index_);
  caf::anon_send(receiver_, op_index_, metric_index_, std::move(r));
}

} // namespace tenzir
