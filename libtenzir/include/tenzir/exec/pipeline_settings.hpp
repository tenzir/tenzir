//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

namespace tenzir::exec {

struct pipeline_settings {
  /// Must be greater than zero.
  duration checkpoint_interval = std::chrono::seconds{10};

  /// How many checkpoints may be in flight at a given time.
  ///
  /// When set to zero, checkpointing is disabled.
  uint64_t checkpoints_in_flight = 1;
};

} // namespace tenzir::exec
