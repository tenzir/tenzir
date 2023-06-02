//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

// Forward declarations
struct accountant_state_impl;

struct accountant_state_deleter {
  void operator()(accountant_state_impl* ptr);
};

struct accountant_state
  : public std::unique_ptr<accountant_state_impl, accountant_state_deleter> {
  using unique_ptr::unique_ptr;

  // Name of the ACCOUNTANT actor.
  static constexpr const char* name = "accountant";
};

/// Accumulates various performance metrics in a key-value format and writes
/// them to VAST table slices.
/// @param self The actor handle.
/// @param cfg The accountant-specific configuration.
/// @param self The root path for relative metric files.
accountant_actor::behavior_type
accountant(accountant_actor::stateful_pointer<accountant_state> self,
           accountant_config cfg, std::filesystem::path root);

} // namespace vast
