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

template <class Handle, class Generator>
caf::actor spawn_generator_source(caf::actor_system& system, Handle dst,
                                  size_t num_elements, Generator generator) {
  using namespace caf;
  struct outer_state {
    /// Name of this actor in log events.
    const char* name = "generator-source";
  };
  auto f = [](stateful_actor<outer_state>* self, Handle dest, size_t n,
              Generator f) {
    using value_type = decltype(f());
    using ds_type = downstream<value_type>;
    struct state {
      size_t i;
      size_t e;
    };
    self->make_source(
      dest,
      [&](state& st) {
        st.i = 0u;
        st.e = n;
      },
      [f{std::move(f)}](state& st, ds_type& out, size_t hint) mutable {
        auto num = std::max(hint, st.e - st.i);
        for (size_t i = 0; i < num; ++i)
          out.push(f());
        st.i += num;
      },
      [](const state& st) { return st.i == st.e; });
  };
  return system.spawn(f, std::move(dst), num_elements, std::move(generator));
}

} // namespace vast::detail

