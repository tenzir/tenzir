//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/command.hpp"
#include "tenzir/detail/tuple_map.hpp"
#include "tenzir/error.hpp"

#include <caf/actor.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>

#include <string>

namespace tenzir {

/// Retrieves the node connection timeout as specified under the option
/// `tenzir.connection-timeout` from the given settings.
auto node_connection_timeout(const caf::settings& options) -> caf::timespan;

/// Look up components by their typed actor interfaces. Returns the first actor
/// of each type passed as template parameter.
template <class... Actors>
auto get_node_components(caf::scoped_actor& self, const node_actor& node)
  -> caf::expected<std::tuple<Actors...>> {
  using result_t = std::tuple<Actors...>;
  auto result = caf::expected{result_t{}};
  auto normalize = [](caf::string_view in) {
    // Remove the uninteresting parts of the name:
    //   tenzir::type_registry_actor -> type_registry
    auto str = std::string{in.data(), in.data() + in.size()};
    str.erase(0, sizeof("tenzir::") - 1);
    str.erase(str.size() - (sizeof("_actor") - 1));
    // Replace '_' with '-': type_registry -> type-registry
    std::replace(str.begin(), str.end(), '_', '-');
    return str;
  };
  auto labels = std::vector<std::string>{
    normalize(caf::type_name_by_id<caf::type_id<Actors>::value>::value)...};
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::move(labels))
    .receive(
      [&](std::vector<caf::actor>& components) {
        result = detail::tuple_map<result_t>(
          std::move(components), []<class Out>(auto&& in) {
            return caf::actor_cast<Out>(std::forward<decltype(in)>(in));
          });
      },
      [&](caf::error& err) { //
        result = caf::make_error(ec::lookup_error,
                                 fmt::format("failed to get components {} from "
                                             "node: {}",
                                             labels, std::move(err)));
      });
  return result;
}

} // namespace tenzir
