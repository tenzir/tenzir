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
  CHECK_EQUAL(dissector->tokens().size(), 3u + 2u);
  auto result = dissector->dissect("1 - 2 - 3");
  REQUIRE(result);
  auto expected = record{{"a", 1u}, {"b", 2u}, {"c", 3u}};
  CHECK_EQUAL(*result, expected);
}
