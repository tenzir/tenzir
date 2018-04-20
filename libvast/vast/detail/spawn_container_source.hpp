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

template <class Handle, class Container>
caf::actor spawn_container_source(caf::actor_system& system, Handle dst,
                                  Container elements) {
  using namespace caf;
  struct outer_state {
    /// Name of this actor in log events.
    const char* name = "container-source";
  };
  auto f = [](stateful_actor<outer_state>* self, Handle dest, Container xs) {
    using iterator = typename Container::iterator;
    using value_type = typename Container::value_type;
    struct state {
      Container xs;
      iterator i;
      iterator e;
    };
    self->make_source(
      dest,
      [&](state& st) {
        st.xs = std::move(xs);
        st.i = st.xs.begin();
        st.e = st.xs.end();
      },
      [](state& st, downstream<value_type>& out, size_t num) {
        size_t pushed = 0;
        while (pushed < num && st.i != st.e) {
          out.push(std::move(*st.i++));
          ++pushed;
        }
      },
      [](const state& st) {
        return st.i == st.e;
      }
    );
  };
  return system.spawn(f, std::move(dst), std::move(elements));
}

} // namespace vast::detail

