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

#include "vast/atoms.hpp"
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
template <class... Names>
caf::expected<std::array<caf::actor, sizeof...(Names)>>
get_node_components(caf::scoped_actor& self, const caf::actor& node,
                    Names&&... names) {
  static_assert(
    std::conjunction_v<std::is_constructible<std::string, Names>...>,
    "name parameter cannot be used to construct a string");
  auto result = caf::expected{std::array<caf::actor, sizeof...(names)>{}};
  auto labels = std::vector<std::string>{std::forward<Names>(names)...};
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::move(labels))
    .receive(
      [&](std::vector<caf::actor>& components) {
        VAST_ASSERT(components.size() == sizeof...(names));
        std::move(components.begin(), components.end(), result->begin());
      },
      [&](caf::error& e) { result = std::move(e); });
  return result;
}

template <class Tuple, class F, class Range, size_t... Is>
Tuple tuple_map_impl(Range&& xs, F&& f, std::index_sequence<Is...>) {
  return std::make_tuple(
    (f.template operator()<std::tuple_element_t<Is, Tuple>>(
      std::forward<decltype(xs[Is])>(xs[Is])))...);
}

/// Turn a random access range into a tuple by applying f to every element of
/// xs. The type of the tuple element at position `n` is supplied for the nth
/// element of xs.
template <class Tuple, class F, class Range>
Tuple tuple_map(Range&& xs, F&& f) {
  constexpr auto tuple_size = std::tuple_size_v<Tuple>;
  VAST_ASSERT(xs.size() == tuple_size);
  return tuple_map_impl<Tuple>(std::forward<Range>(xs), std::forward<F>(f),
                               std::make_index_sequence<tuple_size>{});
}

struct actor_cast_wrapper {
  template <class Out, class In>
  Out operator()(In&& in) {
    return caf::actor_cast<Out>(std::forward<In>(in));
  }
};

/// Look up components by their typed actor interfaces. Returns the first actor
/// of each type passed as template parameter.
template <class... Actors>
caf::expected<std::tuple<Actors...>>
get_typed_node_components(caf::scoped_actor& self, const caf::actor& node) {
  using result_t = std::tuple<Actors...>;
  auto result = caf::expected{result_t{}};
  auto normalize = [](std::string in) {
    // Remove the uninteresting parts of the name:
    //   vast::system::type_registry_actor -> type_registry
    in.erase(0, sizeof("vast::system::") - 1);
    in.erase(in.size() - (sizeof("_actor") - 1));
    // Replace '_' with '-': type_registry -> type-registry
    std::replace(in.begin(), in.end(), '_', '-');
    return in;
  };
  auto labels = std::vector<std::string>{
    normalize(caf::type_name_by_id<caf::type_id<Actors>::value>::value)...};
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::move(labels))
    .receive(
      [&](std::vector<caf::actor> components) {
        result
          = tuple_map<result_t>(std::move(components), actor_cast_wrapper{});
      },
      [&](caf::error& e) { result = std::move(e); });
  return result;
}

} // namespace vast::system
