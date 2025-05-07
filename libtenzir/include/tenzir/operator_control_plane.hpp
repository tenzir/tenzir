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
#include "tenzir/metric_handler.hpp"
#include "tenzir/secret_resolution.hpp"
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

  /// Returns the pipeline's definition.
  virtual auto definition() const noexcept -> std::string_view = 0;

  /// Returns a unique id for the current run.
  virtual auto run_id() const noexcept -> uuid = 0;

  /// Returns the node actor, if the operator location is remote.
  virtual auto node() noexcept -> node_actor = 0;

  /// Returns the operator index.
  virtual auto operator_index() const noexcept -> uint64_t = 0;

  /// Returns the pipeline's diagnostic handler.
  virtual auto diagnostics() noexcept -> diagnostic_handler& = 0;

  /// Returns the pipeline's metric handler for a metric with the type `t`.
  virtual auto metrics(type t) noexcept -> metric_handler = 0;

  /// Returns the metrics receiver actor handle.
  virtual auto metrics_receiver() const noexcept -> metrics_receiver_actor = 0;

  /// Returns whether the pipeline may override its location.
  virtual auto no_location_overrides() const noexcept -> bool = 0;

  /// Returns true if the operator is hosted by process that has a terminal.
  virtual auto has_terminal() const noexcept -> bool = 0;

  /// Returns true if the operator is marked as hidden, i.e., run in the
  /// background.
  virtual auto is_hidden() const noexcept -> bool = 0;

  /// Resolves multiple secrets. The implementation in the
  /// `exec_node_control_plane` will first check the config and then try and
  /// dispatch to the platform plugin. The platform query is async, so this
  /// function will perform `set_waiting(true)`, and only re-schedule the actor
  /// after the request has been fulfilled.
  /// Users MUST `co_yield` after a call to `resolve_secret_must_yield`, but
  /// are guaranteed that resolution is completed once the operator resumes.
  virtual auto resolve_secrets_must_yield(std::vector<secret_request> requests)
    -> void
    = 0;

  auto resolve_secret_must_yield(const located<secret>& secret,
                                 resolved_secret_value& out) -> void {
    return resolve_secrets_must_yield({{secret, out}});
  }

  /// Suspend or resume the operator's runloop. A suspended operator will not
  /// get resumed after it yielded to the executor.
  virtual auto set_waiting(bool value) noexcept -> void = 0;

  /// Return a version of the diagnostic handler that may be passed to other
  /// threads. NOTE: Unlike for the regular diagnostic handler, emitting an
  /// error via the shared diagnostic handler does not shut down the operator
  /// immediately.
  inline auto shared_diagnostics() noexcept -> shared_diagnostic_handler {
    return shared_diagnostic_handler{exec_node_actor{&(self())}};
  }
};

} // namespace tenzir
