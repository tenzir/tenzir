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

  /// Returns the pipeline's unique identifier.
  virtual auto pipeline_id() const noexcept -> std::string_view = 0;

  /// Suspend or resume the operator's runloop. A suspended operator will not
  /// get resumed after it yielded to the executor.
  virtual auto set_waiting(bool value) noexcept -> void = 0;

  static auto noop_final_callback(bool /*success*/) -> failure_or<void> {
    return {};
  }

  using final_callback_t = std::function<failure_or<void>(bool success)>;

  /// The return type of `resolve_secrets_must_yield`. This type ensures a user
  /// yields it by ensuring the conversion operator is called at least once.
  class secret_resolution_sentinel {
  public:
    template <concepts::one_of<std::monostate, chunk_ptr, table_slice> T>
    explicit(false) operator T() && {
      has_yielded_ = true;
      return T{};
    }

    secret_resolution_sentinel() = default;

    secret_resolution_sentinel(const secret_resolution_sentinel&) = delete;
    secret_resolution_sentinel(secret_resolution_sentinel&&) = delete;
    secret_resolution_sentinel& operator=(const secret_resolution_sentinel&)
      = delete;
    secret_resolution_sentinel& operator=(secret_resolution_sentinel&&)
      = delete;

    ~secret_resolution_sentinel() {
      TENZIR_ASSERT_ALWAYS(has_yielded_);
    }

  private:
    bool has_yielded_ = false;
  };

  /// Resolves multiple secrets. The implementation in the
  /// `exec_node_control_plane` will first check the config and then try and
  /// dispatch to the platform plugin. The platform query is async, so this
  /// function will perform `set_waiting(true)`, and only re-schedule the actor
  /// after the request has been successfully fulfilled.
  /// @param requests the requests to resolve
  /// @param final_callback the callback to invoke after all secrets are
  ///        resolved and their callback have been invoked. The `bool` parameter
  ///        will indicate whether resolution was successful so far.
  ///        It is undefined behaviour to do `set_waiting(false)` if resolution
  ///        failed.
  /// @returns a `secret_resolution_sentinel` that must be `co_yield`ed by the
  ///          caller
  [[nodiscard]] virtual auto
  resolve_secrets_must_yield(std::vector<secret_request> requests,
                             final_callback_t final_callback
                             = noop_final_callback)
    -> secret_resolution_sentinel
    = 0;

  /// Return a version of the diagnostic handler that may be passed to other
  /// threads. NOTE: Unlike for the regular diagnostic handler, emitting an
  /// error via the shared diagnostic handler does not shut down the operator
  /// immediately.
  inline auto shared_diagnostics() noexcept -> shared_diagnostic_handler {
    return shared_diagnostic_handler{receiver_actor<diagnostic>{&self()}};
  }
};

} // namespace tenzir
