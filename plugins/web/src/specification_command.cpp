//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/server_command.hpp"

namespace tenzir::plugins::web {

auto openapi_record() -> record {
  auto paths = record{};
  auto schemas = record{};
  for (auto const* plugin : plugins::get<rest_endpoint_plugin>()) {
    auto spec = plugin->openapi_endpoints();
    for (auto& [key, value] : spec)
      paths.emplace(key, value);
    if (auto schemas_spec = plugin->openapi_schemas(); !schemas_spec.empty()) {
      for (auto& [key, value] : schemas_spec)
        schemas.emplace(key, value);
    }
  }
  std::sort(paths.begin(), paths.end(), [](const auto& l, const auto& r) {
    return l.first < r.first;
  });
  std::sort(schemas.begin(), schemas.end(), [](const auto& l, const auto& r) {
    return l.first < r.first;
  });
  // clang-format off
  auto openapi = record{
    {"openapi", "3.0.0"},
    {"info",
     record{
       {"title", "Tenzir Rest API"},
       {"version", "\"0.1\""},
       {"description", R"_(
This API can be used to interact with a Tenzir Node in a RESTful manner.

All API requests must be authenticated with a valid token, which must be
supplied in the `X-Tenzir-Token` request header. The token can be generated
on the command-line using the `tenzir-ctl web generate-token` command.)_"},
     }},
    {"servers", list{{
      record{{"url", "https://tenzir.example.com/api/v0"}},
    }}},
    {"security", list {{
      record {{"TenzirToken", list{}}},
    }}},
    {"components", record{
      {"schemas", std::move(schemas)},
      {"securitySchemes",
        record{{"TenzirToken", record {
            {"type", "apiKey"},
            {"in", "header"},
            {"name", "X-Tenzir-Token"}
        }}}},
    }},
    {"paths", std::move(paths)},
  };
  // clang-format on
  return openapi;
}

auto generate_openapi_json() noexcept -> std::string {
  auto record = openapi_record();
  auto json = to_json(record);
  TENZIR_ASSERT_CHEAP(json);
  return *json;
}

auto generate_openapi_yaml() noexcept -> std::string {
  auto record = openapi_record();
  auto yaml = to_yaml(record);
  TENZIR_ASSERT_CHEAP(yaml);
  return *yaml;
}

auto specification_command(const tenzir::invocation&, caf::actor_system&)
  -> caf::message {
  auto yaml = generate_openapi_yaml();
  fmt::print("---\n{}\n", yaml);
  return {};
}

} // namespace tenzir::plugins::web
