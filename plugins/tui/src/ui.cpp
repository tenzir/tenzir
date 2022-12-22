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

#include <caf/event_based_actor.hpp>

namespace vast::plugins::tui {

using namespace std::chrono_literals;

ui_state::ui_state(ui_actor::stateful_pointer<ui_state> self) : self{self} {
}

void ui_state::hook_logger() {
  auto receiver = caf::actor_cast<caf::actor>(self);
  auto sink = std::make_shared<actor_sink_mt>(std::move(receiver));
  // FIXME: major danger / highly inappropriate. This is not thread safe. We
  // probably want a dedicated logger plugin that allows for adding custom
  // sinks. This yolo approach is only temporary.
  detail::logger()->sinks().push_back(std::move(sink));
}

ui_actor::behavior_type ui(ui_actor::stateful_pointer<ui_state> self) {
  self->state.hook_logger();
  self->set_down_handler([=](const caf::down_msg& msg) {
    self->quit(msg.reason);
  });
  auto* tui = &self->state.tui;
  return {
    [=](std::string log) {
      // Warning: do not use the VAST_* log macros in this function. It will
      // cause an infite loop because this handler is called for every log
      // message.
      tui->add_log(std::move(log));
      tui->redraw();
    },
    [=](atom::run) {
      // Ban UI into dedicated thread. We're getting a down message upon
      // termination, e.g., when pushes the exit button or CTRL+C.
      self->spawn<caf::detached + caf::monitored>([tui] {
        tui->loop();
      });
    },
  };
}

} // namespace vast::plugins::tui
