//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/dissector.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST(dissect) {
  auto dissector
    = dissector::make("%{a} - %{b} - %{c}", dissector_style::dissect);
  REQUIRE(dissector);
  auto result = dissector->dissect("1 - 2 - 3");
  REQUIRE(result);
  auto expected = record{{"a", 1u}, {"b", 2u}, {"c", 3u}};
  CHECK_EQUAL(*result, expected);
  CHECK_EQUAL(dissector->tokens().size(), 3u + 2u);
}

TEST(dissect optional) {
  auto dissector
    = dissector::make("%{?a} - %{} - %{c}", dissector_style::dissect);
  REQUIRE(dissector);
  auto result = dissector->dissect("1 - 2 - 3");
  REQUIRE(result);
  auto expected = record{{"c", 3u}};
  CHECK_EQUAL(*result, expected);
  const auto& tokens = dissector->tokens();
  REQUIRE_EQUAL(tokens.size(), 3u + 2u);
  const auto& first = std::get<dissector::field>(tokens.at(0));
  CHECK_EQUAL(first.name, "a");
  const auto& third = std::get<dissector::field>(tokens.at(2));
  CHECK(third.name.empty());
}
