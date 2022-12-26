//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/ui.hpp"

#include <vast/command.hpp>

#include <caf/scoped_actor.hpp>

namespace vast::plugins::tui {

caf::message
tui_command(const invocation& /* inv */, caf::actor_system& system) {
  auto ui = spawn_ui(system);
  caf::scoped_actor self{system};
  self->send(ui, atom::run_v);
  self->wait_for(ui);
  return {};
}

} // namespace vast::plugins::tui
