//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_or_connect_to_node.hpp"

#include "tenzir/connect_to_node.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/spawn_node.hpp"

#include <caf/settings.hpp>

namespace tenzir {

std::variant<caf::error, node_actor, scope_linked<node_actor>>
spawn_or_connect_to_node(caf::scoped_actor& self, const caf::settings& opts,
                         const caf::settings& node_opts) {
  TENZIR_TRACE_SCOPE("{}", TENZIR_ARG(opts));
  // In case we have a node actor in the process we get that.
  // TODO: Iff we are in the connect case we should connect via the endpoint
  //       as well and verify that the local node actor id is the same instead
  //       of blindly returning the handle.
  if (auto node = self->system().registry().get<node_actor>("tenzir.node"))
    return node;
  auto convert = [](auto&& result)
    -> std::variant<caf::error, node_actor, scope_linked<node_actor>> {
    if (result)
      return std::move(*result);
    return std::move(result.error());
  };
  if (caf::get_or<bool>(opts, "tenzir.node", false))
    return convert(spawn_node(self, node_opts));
  return convert(connect_to_node(self, opts));
}

} // namespace tenzir
