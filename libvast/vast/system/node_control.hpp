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

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>

#include <array>
#include <string>
#include <string_view>

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

/// Look up components by category. Returns the first actor of each
/// category name passed in `names`.
template <size_t N>
caf::expected<std::array<caf::actor, N>>
get_node_components(caf::scoped_actor& self, caf::actor node,
                    const std::string_view (&names)[N]) {
  auto result = caf::expected{std::array<caf::actor, N>{}};
  std::vector<std::string> labels;
  labels.reserve(N);
  for (auto name : names)
    labels.emplace_back(name);
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::move(labels))
    .receive(
      [&](std::vector<caf::actor>& components) {
        VAST_ASSERT(components.size() == N);
        std::move(components.begin(), components.end(), result->begin());
      },
      [&](caf::error& e) { result = std::move(e); });
  return result;
}

} // namespace vast::system
