/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/stop_command.hpp"

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
  VAST_TRACE("{}", detail::id_or_name(inv));
  // Bail out early for bogus invocations.
  if (caf::get_or(inv.options, "vast.node", false))
    return caf::make_message(caf::make_error(
      ec::invalid_configuration, "cannot start and immediately stop a node"));
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
    [&](caf::error& e) { err = std::move(e); });
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
