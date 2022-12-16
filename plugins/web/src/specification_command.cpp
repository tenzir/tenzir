//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/server_command.hpp"

namespace vast::plugins::web {

auto specification_command(const vast::invocation&, caf::actor_system&)
  -> caf::message {
  auto paths = record{};
  for (auto const* plugin : plugins::get<rest_endpoint_plugin>()) {
    auto spec = plugin->openapi_specification();
    VAST_ASSERT_CHEAP(caf::holds_alternative<record>(spec));
    for (auto& [key, value] : caf::get<record>(spec))
      paths.emplace(key, value);
  }
  // clang-format off
  auto openapi = record{
    {"openapi", "3.0.0"},
    {"info",
     record{
       {"title", "VAST Rest API"},
       {"version", "\"0.1\""},
       {"description", R"_(
This API can be used to interact with a VAST Node in a RESTful manner.

All API requests must be authenticated with a valid token, which must be
supplied in the `X-VAST-Token` request header. The token can be generated
on the command-line using the `vast rest generate-token` command.)_"},
     }},
    {"servers", list{{
      record{{"url", "https://vast.example.com/api/v0"}},
    }}},
    {"security", list {{
      record {{"VastToken", list{}}},
    }}},
    {"components", record{
      {"securitySchemes",
        record{{"VastToken", record {
            {"type", "apiKey"},
            {"in", "header"},
            {"name", "X-VAST-Token"}
        }}}},
    }},
    {"paths", std::move(paths)},
  };
  // clang-format on
  auto yaml = to_yaml(openapi);
  VAST_ASSERT_CHEAP(yaml);
  fmt::print("---\n{}\n", *yaml);
  return {};
}

} // namespace vast::plugins::web
