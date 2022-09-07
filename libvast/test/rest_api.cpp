//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE rest_api

#include <vast/plugin.hpp>
#include <vast/test/test.hpp>

TEST(OpenAPI specs) {
  for (auto const& plugin : vast::plugins::get()) {
    auto const* rest_plugin = plugin.as<vast::rest_endpoint_plugin>();
    if (!rest_plugin)
      continue;
    auto spec = rest_plugin->openapi_specification();
    auto parsed_spec = vast::from_yaml(spec);
    REQUIRE_NOERROR(parsed_spec);
    REQUIRE(caf::holds_alternative<vast::record>(parsed_spec->get_data()));
    // auto
    auto endpoints = rest_plugin->api_endpoints();
  }
}
