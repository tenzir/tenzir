//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/metrics.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"

#include <caf/typed_actor.hpp>

namespace tenzir {

/// The operator control plane is the bridge between an operator and an
/// executor, and serves as an escape hatch for the operator into an outer
/// context like the actor system.
struct operator_control_plane {
  /// Destroy the operator control plane.
  virtual ~operator_control_plane() noexcept = default;

  /// Returns the hosting actor.
  virtual auto self() noexcept -> exec_node_actor::base& = 0;

  /// Returns the node actor, if the operator location is remote.
  virtual auto node() noexcept -> node_actor = 0;

  /// Returns the pipeline's diagnostic handler.
  virtual auto diagnostics() noexcept -> diagnostic_handler& = 0;

  /// Returns the pipeline's metric handler.
  virtual auto metrics() noexcept -> metric_handler& = 0;

  /// Returns whether the pipeline may override its location.
  virtual auto no_location_overrides() const noexcept -> bool = 0;

  /// Returns true if the operator is hosted by process that has a terminal.
  virtual auto has_terminal() const noexcept -> bool = 0;

  /// Suspend or resume the operator's runloop. A suspended operator will not
  /// get resumed after it yielded to the executor.
  virtual auto set_waiting(bool value) noexcept -> void = 0;

  /// Return a version of the diagnostic handler that may be passed to other
  /// threads. NOTE: Unlike for the regular diagnostic handler, emitting an
  /// erorr via the shared diagnostic handler does not shut down the operator
  /// immediately.
  inline auto shared_diagnostics() noexcept -> shared_diagnostic_handler {
    return shared_diagnostic_handler{exec_node_actor{&(self())}};
  }
};

} // namespace tenzir
