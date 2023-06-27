//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/spawn_or_connect_to_node.hpp"

#include "vast/connect_to_node.hpp"
#include "vast/logger.hpp"
#include "vast/spawn_node.hpp"

#include <caf/settings.hpp>

namespace vast {

std::variant<caf::error, node_actor, scope_linked<node_actor>>
spawn_or_connect_to_node(caf::scoped_actor& self, const caf::settings& opts,
                         const caf::settings& node_opts) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(opts));
  auto convert = [](auto&& result)
    -> std::variant<caf::error, node_actor, scope_linked<node_actor>> {
    if (result)
      return std::move(*result);
    return std::move(result.error());
  };
  if (caf::get_or<bool>(opts, "tenzir.node", false))
    return convert(spawn_node(self, node_opts));
  return convert(connect_to_node(self, node_opts));
}

} // namespace vast
