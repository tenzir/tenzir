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

template <size_t N>
caf::expected<std::array<caf::actor, N>>
get_node_components(caf::scoped_actor& self, caf::actor node,
                    const char* const (&names)[N]) {
  auto result = caf::expected{std::array<caf::actor, N>{}};
  self->request(node, caf::infinite, get_atom::value)
    .receive(
      [&](const std::string& id, system::registry& reg) {
        auto find_actor = [&](std::string_view name) -> caf::actor {
          if (auto er = reg.components[id].find(name);
              er != reg.components[id].end())
            return er->second.actor;
          return nullptr;
        };
        for (size_t i = 0; i < N; ++i) {
          result->at(i) = find_actor(names[i]);
        }
      },
      [&](caf::error& e) { result = std::move(e); });
  return result;
}

} // namespace vast::system
