//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/aliases.hpp"

#include <caf/typed_actor.hpp>

namespace tenzir {

struct operator_metric;

class metric_handler {
public:
  metric_handler() = default;
  metric_handler(metrics_receiver_actor receiver, uint64_t operator_index,
                 uint64_t metric_index, const type& metric_type);

  auto emit(record&& r) -> void;

private:
  metrics_receiver_actor receiver_ = {};
  uint64_t op_index_ = {};
  uint64_t metric_index_ = {};
};

} // namespace tenzir
