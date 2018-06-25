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

#pragma once

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>

namespace vast::detail {

/// Spawns an actor that streams all elements from `container` to all sinks.
template <class Container, class Handle, class... Handles>
caf::actor spawn_container_source(caf::actor_system& system,
                                  Container container, Handle sink,
                                  Handles... sinks) {
  using namespace caf;
  struct outer_state {
    /// Name of this actor in log events.
    const char* name = "container-source";
  };
  auto f = [](stateful_actor<outer_state>* self, Container xs,
              Handle y, Handles... ys) {
    using iterator = typename Container::iterator;
    using value_type = typename Container::value_type;
    struct state {
      Container xs;
      iterator i;
      iterator e;
    };
    auto mgr = self->make_source(
      std::move(y),
      [&](state& st) {
        st.xs = std::move(xs);
        st.i = st.xs.begin();
        st.e = st.xs.end();
      },
      [](state& st, downstream<value_type>& out, size_t hint) {
        auto n = std::min(hint, static_cast<size_t>(std::distance(st.i, st.e)));
        for (size_t pushed = 0; pushed < n; ++pushed)
          out.push(std::move(*st.i++));
      },
      [](const state& st) {
        return st.i == st.e;
      }
    ).ptr();
    if constexpr (sizeof...(Handles) > 0)
      std::make_tuple(mgr->add_outbound_path(ys)...);
  };
  return system.spawn(f, std::move(container), std::move(sink),
                      std::move(sinks)...);
}

} // namespace vast::detail

