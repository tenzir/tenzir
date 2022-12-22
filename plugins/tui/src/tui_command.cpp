//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/ui.hpp"

#include <caf/scoped_actor.hpp>
#include <vast/command.hpp>
#include <vast/logger.hpp>

namespace vast::plugins::tui {

caf::message
tui_command(const invocation& /* inv */, caf::actor_system& system) {
  auto self = caf::scoped_actor{system};
  auto actor = self->spawn<caf::detached>(ui);
  self->send(actor, atom::run_v);
  self->wait_for(actor);
  return {};
}

} // namespace vast::plugins::tui
