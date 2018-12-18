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

#include <type_traits>

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/is_actor_handle.hpp>
#include <caf/is_typed_actor.hpp>
#include <caf/typed_actor.hpp>

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
    actor first_sink;
    if constexpr (is_actor_handle<Handle>::value) {
      first_sink = actor_cast<actor>(std::move(y));
    } else {
      // Assume a container of actor handles.
      first_sink = actor_cast<actor>(std::move(y.front()));
      y.erase(y.begin());
    }
    auto mgr = self->make_source(
      std::move(first_sink),
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
    auto add = [&](auto& x) {
      if constexpr (is_actor_handle<std::decay_t<decltype(x)>>::value)
        mgr->add_outbound_path(x);
      else
        for (auto& hdl : x)
          mgr->add_outbound_path(hdl);
    };
    if constexpr (!is_actor_handle<Handle>::value)
      add(y);
    (add(ys), ...);
  };
  return system.spawn(f, std::move(container), std::move(sink),
                      std::move(sinks)...);
}

} // namespace vast::detail

