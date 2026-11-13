//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/connect_to_node.hpp"

#include "tenzir/test/test.hpp"

#include <caf/settings.hpp>

#include <string>

using namespace tenzir;

TEST("node endpoint - default") {
  auto opts = caf::settings{};
  auto endpoint = detail::get_node_endpoint(opts);
  REQUIRE(endpoint);
  CHECK_EQUAL(endpoint->host, "127.0.0.1");
  CHECK_EQUAL(endpoint->port->number(), 5158);
}

TEST("node endpoint - explicit") {
  auto opts = caf::settings{};
  caf::put(opts, "tenzir.endpoint", "example.org:42");
  auto endpoint = detail::get_node_endpoint(opts);
  REQUIRE(endpoint);
  CHECK_EQUAL(endpoint->host, "example.org");
  CHECK_EQUAL(endpoint->port->number(), 42);
}

TEST("node endpoint - enabled") {
  for (auto enabled : {caf::config_value{true}, caf::config_value{"true"}}) {
    auto opts = caf::settings{};
    caf::put(opts, "tenzir.endpoint", enabled);
    auto endpoint = detail::get_node_endpoint(opts);
    REQUIRE(endpoint);
    CHECK_EQUAL(endpoint->host, "127.0.0.1");
    CHECK_EQUAL(endpoint->port->number(), 5158);
  }
}

TEST("node endpoint - disabled") {
  for (auto disabled : {caf::config_value{false}, caf::config_value{"false"}}) {
    auto opts = caf::settings{};
    caf::put(opts, "tenzir.endpoint", disabled);
    auto endpoint = detail::get_node_endpoint(opts);
    REQUIRE(not endpoint);
    CHECK_EQUAL(endpoint.error(), ec::invalid_configuration);
  }
}
