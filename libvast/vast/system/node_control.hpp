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

#include "vast/detail/array.hpp"
#include "vast/error.hpp"
#include "vast/system/tracker.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>

#include <array>

namespace vast::system {

template <typename... Arguments>
caf::expected<caf::actor>
spawn_at_node(caf::scoped_actor& self, caf::actor node, Arguments&&... xs) {
  caf::expected<caf::actor> result = caf::no_error;
  self->request(node, caf::infinite, std::forward<Arguments>(xs)...)
    .receive([&](caf::actor& a) { result = std::move(a); },
             [&](caf::error& e) { result = std::move(e); });
  return result;
}

template <typename... Atoms>
auto get_node_component(caf::scoped_actor& self, caf::actor node) {
  auto result = caf::expected{
    detail::generate_array<sizeof...(Atoms), caf::expected<caf::actor>>(
      caf::no_error)};
  self->request(node, caf::infinite, get_atom::value)
    .receive(
      [&](const std::string& id, system::registry& reg) {
        auto find_actor = [&](auto atom) -> caf::expected<caf::actor> {
          auto er = reg.components[id].find(to_string(atom));
          if (er == reg.components[id].end())
            return make_error(ec::missing_component, to_string(atom));
          return er->second.actor;
        };
        size_t i = 0;
        (..., void((*result)[i++] = find_actor(Atoms{})));
      },
      [&](caf::error& e) { result = std::move(e); });
  return result;
}

} // namespace vast::system
