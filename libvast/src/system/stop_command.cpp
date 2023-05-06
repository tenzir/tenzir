//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/stop_command.hpp"

#include "vast/command.hpp"
#include "vast/data.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/connect_to_node.hpp"

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

namespace vast::system {

caf::message stop_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE_SCOPE("{}", inv);
  // Bail out early for bogus invocations.
  if (caf::get_or(inv.options, "vast.node", false))
    return caf::make_message(
      caf::make_error(ec::invalid_configuration,
                      "unable to run 'vast stop' when spawning a "
                      "node locally instead of connecting to one; please "
                      "unset the option vast.node"));
  // Obtain VAST node.
  caf::scoped_actor self{sys};
  auto node = connect_to_node(self, content(sys.config()));
  if (!node)
    return caf::make_message(std::move(node.error()));
  self->monitor(*node);
  VAST_INFO("requesting remote shutdown");
  caf::error err;
  self->send_exit(*node, caf::exit_reason::user_shutdown);
  self->receive(
    [&](const caf::down_msg&) {
      VAST_INFO("remote node terminated successfully");
    },
    [&](caf::error& e) {
      err = std::move(e);
    });
  if (err)
    return caf::make_message(std::move(err));
  return {};
}

} // namespace vast::system
