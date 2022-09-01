//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/event_based_actor.hpp>

namespace vast::detail {

/// Spawns an actor that streams the first `num_elements` items produced by
/// `generator` to all sinks.
template <class Generator, class Handle, class... Handles>
caf::actor spawn_generator_source(caf::actor_system& system,
                                  size_t num_elements, Generator generator,
                                  Handle sink, Handles... sinks) {
  using namespace caf;
  struct outer_state {
    /// Name of this actor in log events.
    const char* name = "generator-source";
  };
  auto f = [](stateful_actor<outer_state>* self, size_t n, Generator f,
              Handle y, Handles... ys) {
    using value_type = decltype(f());
    using ds_type = downstream<value_type>;
    auto mgr = caf::attach_stream_source(
                 self, std::move(y),
                 [&](size_t& remaining) {
                   remaining = n;
                 },
                 [f{std::move(f)}](size_t& remaining, ds_type& out,
                                   size_t hint) mutable {
                   auto n = std::min(hint, remaining);
                   for (size_t pushed = 0; pushed < n; ++pushed)
                     out.push(f());
                   remaining -= n;
                 },
                 [](const size_t& remaining) {
                   return remaining == 0;
                 })
                 .ptr();
    if constexpr (sizeof...(Handles) > 0)
      std::make_tuple(mgr->add_outbound_path(ys)...);
  };
  return system.spawn(f, num_elements, std::move(generator),
                      std::move(sink), std::move(sinks)...);
}

} // namespace vast::detail

