//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"

#include <string_view>

using namespace tenzir;

TEST("tql2 parser: expression stream parses multiple records") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto source = std::string_view{"{x:1}\n{x:2}"};
  auto parsed = parse_expression_stream_with_bad_diagnostics(
    source, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{2});
  CHECK_EQUAL(parsed->bytes_consumed, source.size());
  CHECK(try_as<ast::record>(parsed->expressions[0]));
  CHECK(try_as<ast::record>(parsed->expressions[1]));
}

TEST("tql2 parser: expression stream handles whitespace separators") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto source = std::string_view{"{x:1}   {x:2}\n\n{x:3}"};
  auto parsed = parse_expression_stream_with_bad_diagnostics(
    source, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{3});
  CHECK_EQUAL(parsed->bytes_consumed, source.size());
}

TEST("tql2 parser: expression stream leaves incomplete trailing suffix") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto source = std::string_view{"{x:1}\n{x:"};
  auto parsed = parse_expression_stream_with_bad_diagnostics(
    source, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{1});
  CHECK_EQUAL(source.substr(parsed->bytes_consumed), std::string_view{"{x:"});
}
