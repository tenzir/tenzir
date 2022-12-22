//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/ui.hpp"

#include "tui/actor_sink.hpp"

#include <vast/logger.hpp>

namespace vast::plugins::tui {

ui_state::ui_state(ui_actor::stateful_pointer<ui_state> self) : self{self} {
}

void ui_state::hook_logger() {
  auto receiver = caf::actor_cast<caf::actor>(self);
  auto sink = std::make_shared<actor_sink_mt>(receiver);
  // FIXME: major danger. This is not thread safe. We probably want a dedicated
  // logger plugin that allows for adding custom sinks.
  detail::logger()->sinks().push_back(std::move(sink));
}

void ui_state::loop() {
  tui.loop();
}

ui_actor::behavior_type ui(ui_actor::stateful_pointer<ui_state> self) {
  self->state.hook_logger();
  return {[self](atom::run) {
    self->state.loop();
  }};
}

} // namespace vast::plugins::tui
