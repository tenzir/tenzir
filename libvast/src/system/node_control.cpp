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
#include "vast/system/configuration.hpp"

#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/variant.hpp>

namespace vast::system {

auto node_connection_timeout(const caf::settings& options) -> caf::timespan {
  auto timeout_value
    = system::get_or_duration(options, "vast.connection-timeout",
                              defaults::system::node_connection_timeout);
  if (!timeout_value) {
    VAST_ERROR("client failed to read connection-timeout: {}",
               timeout_value.error());
    return caf::timespan{defaults::system::node_connection_timeout};
  }
  auto timeout = caf::timespan{*timeout_value};
  if (timeout == timeout.zero())
    return caf::infinite;
  return timeout;
}

caf::expected<caf::actor>
spawn_at_node(caf::scoped_actor& self, const node_actor& node, invocation inv) {
  const auto timeout = node_connection_timeout(self->config().content);
  caf::expected<caf::actor> result = caf::error{};
  self->request(node, timeout, atom::spawn_v, inv)
    .receive(
      [&](caf::actor& actor) {
        result = std::move(actor);
      },
      [&](caf::error& err) {
        result = caf::make_error(ec::unspecified,
                                 fmt::format("failed to spawn '{}' at node: {}",
                                             inv.full_name, err));
      });
  return result;
}

} // namespace vast::system
