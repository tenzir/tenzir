//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "rest/server_command.hpp"

namespace vast::plugins::rest {

auto specification_command(const vast::invocation&, caf::actor_system&)
  -> caf::message {
  auto paths = record{};
  for (auto const* plugin : plugins::get<rest_endpoint_plugin>()) {
    auto spec = plugin->openapi_specification();
    VAST_ASSERT_CHEAP(caf::holds_alternative<record>(spec));
    for (auto& [key, value] : caf::get<record>(spec))
      paths.emplace(key, value);
  }
  auto openapi = record{
    {"openapi", "3.0.0"},
    {"info",
     record{
       {"description", "VAST API"},
       {"version", "0.1"},
     }},
    {"paths", std::move(paths)},
  };
  auto yaml = to_yaml(openapi);
  VAST_ASSERT_CHEAP(yaml);
  fmt::print("---\n{}\n", *yaml);
  return {};
}

} // namespace vast::plugins::rest
