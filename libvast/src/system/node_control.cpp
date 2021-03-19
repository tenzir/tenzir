//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/node_control.hpp"

#include "vast/detail/overload.hpp"

#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/variant.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_at_node(caf::scoped_actor& self, node_actor node, invocation inv) {
  caf::expected<caf::actor> result = caf::no_error;
  self->request(node, caf::infinite, atom::spawn_v, std::move(inv))
    .receive([&](caf::actor actor) { result = std::move(actor); },
             [&](caf::error err) { result = std::move(err); });
  return result;
}

} // namespace vast::system
