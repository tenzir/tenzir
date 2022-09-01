//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/fwd.hpp>

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/actor_system.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/is_actor_handle.hpp>
#include <caf/is_typed_actor.hpp>
#include <caf/typed_actor.hpp>

#include <type_traits>

namespace vast::detail {

/// Spawns an actor that streams all elements from `container` to all sinks.
template <class Container, class Handle, class... Handles>
caf::actor
spawn_container_source(caf::actor_system& system, Container container,
                       Handle sink, Handles... sinks) {
  using namespace caf;
  struct outer_state {
    /// Name of this actor in log events.
    const char* name = "container-source";
  };
  auto f = [](stateful_actor<outer_state>* self, Container xs, Handle y,
              Handles... ys) {
    using iterator = typename Container::iterator;
    using sentinel = decltype(xs.end());
    using value_type = typename Container::value_type;
    struct state {
      Container xs;
      iterator i;
      sentinel e;
    };
    actor first_sink;
    if constexpr (is_actor_handle<Handle>::value) {
      first_sink = actor_cast<actor>(std::move(y));
    } else {
      // Assume a container of actor handles.
      first_sink = actor_cast<actor>(std::move(y.front()));
      y.erase(y.begin());
    }
    // clang-format off
    auto mgr = caf::attach_stream_source(self,
      std::move(first_sink),
      [&](state& st) {
        st.xs = std::move(xs);
        st.i = st.xs.begin();
        st.e = st.xs.end();
      },
      [](state& st, downstream<value_type>& out, size_t hint) {
        for (size_t i = 0; i < hint && st.i != st.e; ++i, ++st.i)
          if constexpr (std::is_same_v<value_type, vast::table_slice>)
            out.push((*st.i).unshare());
          else
            out.push(*st.i);
      },
      [](const state& st) {
        return st.i == st.e;
      }
    ).ptr();
    // clang-format on
    [[maybe_unused]] auto add = [&](auto& x) {
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
