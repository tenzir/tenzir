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

// TODO: pick juicy examples from the official test suites at the links below
// and ensure compatibility.
//
// https://github.com/logstash-plugins/logstash-filter-dissect/blob/main/spec/filters/dissect_spec.rb
// https://github.com/logstash-plugins/logstash-filter-dissect/blob/main/spec/fixtures/dissect_tests.json

TEST(logstash - when the delimiters contain braces) {
  auto dissector
    = dissector::make("{%{a}}{%{b}}%{rest}", dissector_style::dissect);
  REQUIRE(dissector);
  auto result = dissector->dissect("{foo}{bar}");
  REQUIRE(result);
  auto expected = record{{"a", "foo"}, {"b", "bar"}, {"rest", {}}};
  CHECK_EQUAL(*result, expected);
}

TEST(logstash - basic dissection like CSV with missing fields) {
  auto pattern
    = R"__([%{occurred_at}] %{code} %{service} values: "%{v1}","%{v2}","%{v3}"%{rest})__";
  auto dissector = dissector::make(pattern, dissector_style::dissect);
  auto message
    = R"__([25/05/16 09:10:38:425 BST] 00000001 SystemOut values: "f1","","f3")__";
  auto result = dissector->dissect(message);
  REQUIRE(result);
  CHECK_EQUAL(result->at("occurred_at"), "25/05/16 09:10:38:425 BST");
  CHECK_EQUAL(result->at("code"), "00000001");
  CHECK_EQUAL(result->at("service"), "SystemOut");
  CHECK_EQUAL(result->at("v1"), "f1");
  CHECK_EQUAL(result->at("v2"), "");
  CHECK_EQUAL(result->at("v3"), "f3");
  CHECK_EQUAL(result->at("rest"), data{});
}
