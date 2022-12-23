//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// Returns a sigset_t with SIGINT and SIGTERM.
sigset_t termsigset();

struct signal_reflector_state {
  constexpr static inline auto name = "signal-reflector";

  /// Marks whether the listener already relayed a signal.
  bool got_signal = false;

  /// An optional handler actor that orchestrates a graceful shutdown.
  termination_handler_actor handler = {};
};

/// @param self The actor handle.
signal_reflector_actor::behavior_type signal_reflector(
  signal_reflector_actor::stateful_pointer<signal_reflector_state> self);

} // namespace vast::system
