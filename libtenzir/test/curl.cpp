//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/curl.hpp"

#include "tenzir/collect.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST(overwriting HTTP headers) {
  auto easy = curl::easy{};
  easy.set_http_header("Foo", "42");
  auto headers = collect(easy.headers());
  CHECK_EQUAL(headers.size(), 1ull);
  easy.set_http_header("Foo", "Bar");
  headers = collect(easy.headers());
  REQUIRE_EQUAL(headers.size(), 1ull);
  auto [name, value] = headers[0];
  CHECK_EQUAL(name, "Foo");
  CHECK_EQUAL(value, "Bar");
}
