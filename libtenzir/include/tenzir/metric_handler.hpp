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
#include "tenzir/detail/weak_handle.hpp"

namespace tenzir {

struct operator_metric;

class metric_handler {
public:
  metric_handler() = default;
  metric_handler(detail::weak_handle<metrics_receiver_actor> new_receiver,
                 uint64_t operator_index, type metric_type);

  auto emit(record&& r) -> void;
  auto emit(operator_metric&& m) -> void;

private:
  detail::weak_handle<metrics_receiver_actor> receiver_;
  uint64_t op_index_ = {};
  type metric_type_ = {};
};

} // namespace tenzir
