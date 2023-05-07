//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/atoms.hpp"
#include "vast/command.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/tuple_map.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/actors.hpp"

#include <caf/actor.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>

#include <array>
#include <string>
#include <string_view>

namespace vast::system {

/// Retrieves the node connection timeout as specified under the option
/// `vast.connection-timeout` from the given settings.
caf::timespan node_connection_timeout(const caf::settings& options);

caf::expected<caf::actor>
spawn_at_node(caf::scoped_actor& self, const node_actor& node, invocation inv);

/// Look up components by their typed actor interfaces. Returns the first actor
/// of each type passed as template parameter.
template <class... Actors>
caf::expected<std::tuple<Actors...>>
get_node_components(caf::scoped_actor& self, const node_actor& node) {
  using result_t = std::tuple<Actors...>;
  auto result = caf::expected{result_t{}};
  auto normalize = [](caf::string_view in) {
    // Remove the uninteresting parts of the name:
    //   vast::system::type_registry_actor -> type_registry
    auto str = std::string{in.data(), in.data() + in.size()};
    str.erase(0, sizeof("vast::system::") - 1);
    str.erase(str.size() - (sizeof("_actor") - 1));
    // Replace '_' with '-': type_registry -> type-registry
    std::replace(str.begin(), str.end(), '_', '-');
    return str;
  };
  const auto timeout = node_connection_timeout(self->config().content);
  auto labels = std::vector<std::string>{
    normalize(caf::type_name_by_id<caf::type_id<Actors>::value>::value)...};
  self->request(node, timeout, atom::get_v, atom::label_v, labels)
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

} // namespace vast::system
