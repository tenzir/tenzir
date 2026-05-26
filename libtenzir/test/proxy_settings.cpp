//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/proxy_settings.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("no-proxy exact host entries do not match subdomains") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{"example.com"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("example.com"));
  CHECK(not bypass_proxy("api.example.com"));
}

TEST("no-proxy dotted entries match subdomains") {
  auto settings = caf::settings{};
  caf::put(settings, "tenzir.no-proxy", std::string{".example.com"});
  REQUIRE_EQUAL(initialize_proxy_settings(settings), caf::error{});
  CHECK(bypass_proxy("example.com"));
  CHECK(bypass_proxy("api.example.com"));
}
