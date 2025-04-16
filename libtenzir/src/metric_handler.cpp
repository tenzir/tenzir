//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/metric_handler.hpp"

#include <caf/anon_mail.hpp>
#include <caf/send.hpp>

namespace tenzir {

metric_handler::metric_handler(metrics_receiver_actor receiver,
                               uint64_t operator_index, const type& metric_type)
  : receiver_{std::move(receiver)}, op_index_{operator_index} {
  caf::anon_mail(op_index_, metrics_id_, type{metric_type, {{"internal", ""}}})
    .send(receiver_);
}

auto metric_handler::emit(record&& r) -> void {
  // Explicitly create a data-from-time cast here to support macOS builds.
  r.emplace("timestamp", time{time::clock::now()});
  r.emplace("operator_id", op_index_);
  caf::anon_mail(op_index_, metrics_id_, std::move(r)).send(receiver_);
}

} // namespace tenzir
