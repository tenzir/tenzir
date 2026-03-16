//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"

#include <caf/fwd.hpp>
#include <caf/response_promise.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <vector>

namespace tenzir::policy {

struct sequential;
struct parallel;

} // namespace tenzir::policy

namespace tenzir {

struct terminator_state {
  std::vector<caf::actor> remaining_actors;
  caf::typed_response_promise<atom::done> promise;
  static inline const char* name = "terminator";
};

/// Performs a parallel shutdown of a list of actors.
/// @param self The terminator actor.
template <class Policy>
terminator_actor::behavior_type
terminator(terminator_actor::stateful_pointer<terminator_state> self);

} // namespace tenzir
