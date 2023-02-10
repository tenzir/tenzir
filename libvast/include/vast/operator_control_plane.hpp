//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/taxonomies.hpp"
#include "vast/type.hpp"

namespace vast {

/// The operator control plane is the bridge between an operator and an
/// executor, and serves as an escape hatch for the operator into an outer
/// context like the actor system.
struct operator_control_plane {
  /// Destroy the operator control plane.
  virtual ~operator_control_plane() noexcept = default;

  /// Returns the hosting actor. This function is supposed to terminate if the
  /// hosting actor is no longer alive, and must only be called from within the
  /// operator.
  [[nodiscard]] virtual auto self() noexcept -> caf::event_based_actor& = 0;

  /// Stop the execution of the operator.
  virtual auto stop(caf::error error = {}) noexcept -> void = 0;

  /// Emit a warning that gets transported via the executor's side-channel.
  /// An executor may treat warnings as errors. Warnings additionally get
  /// reported to the executor's side-channel as `vast.warning` events.
  virtual auto warn(caf::error warning) noexcept -> void = 0;

  /// Emit events to the executor's side-channel, e.g., metrics.
  virtual auto emit(table_slice metrics) noexcept -> void = 0;

  /// Returns the downstream demand for a given schema in terms of number of
  /// elements. If no schema is provided, returns general demand for all
  /// schemas.
  [[nodiscard]] virtual auto demand(type schema = {}) const noexcept -> size_t
    = 0;

  /// Access available schemas.
  [[nodiscard]] virtual auto schemas() const noexcept
    -> const std::vector<type>& = 0;

  /// Access available concepts.
  [[nodiscard]] virtual auto concepts() const noexcept
    -> const concepts_map& = 0;
};

} // namespace vast
