//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/node_control.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/detail/overload.hpp"

#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/variant.hpp>

namespace vast::system {

caf::duration node_connection_timeout(const caf::settings& options) {
  auto timeout = caf::duration{defaults::system::node_connection_timeout};
  if (auto connection_timeout_arg
      = caf::get_if<std::string>(&options, "vast.connection-timeout")) {
    if (auto batch_timeout = to<duration>(*connection_timeout_arg))
      timeout = caf::duration{*batch_timeout};
    else
      VAST_WARN("client cannot set vast.connection-timeout to {} as it "
                "is not a valid duration",
                *connection_timeout_arg);
  }
  if (timeout.is_zero())
    timeout = caf::infinite;
  return timeout;
}

caf::expected<caf::actor>
spawn_at_node(caf::scoped_actor& self, const node_actor& node, invocation inv) {
  const auto timeout = node_connection_timeout(self->config().content);
  caf::expected<caf::actor> result = caf::no_error;
  self->request(node, timeout, atom::spawn_v, inv)
    .receive(
      [&](caf::actor& actor) {
        result = std::move(actor);
      },
      [&](caf::error& err) {
        result
          = caf::make_error(ec::unspecified, "failed to spawn '{}' at node: {}",
                            inv.full_name, err);
      });
  return result;
}

} // namespace vast::system
