//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"

#include <string>
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

TEST("tql2 parser: expression stream accepts partial UTF-8 suffix") {
  auto first_chunk = std::string{R"({msg: ")"};
  first_chunk.push_back(static_cast<char>(0xc3));
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto parsed = parse_expression_stream_with_bad_diagnostics(
    first_chunk, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{0});
  CHECK_EQUAL(parsed->bytes_consumed, size_t{0});
  CHECK(not parsed->has_error);
  CHECK(dh.empty());

  first_chunk.push_back(static_cast<char>(0xa4));
  first_chunk += "\"}";
  auto full_source = std::string_view{first_chunk};
  auto parsed_full = parse_expression_stream_with_bad_diagnostics(
    full_source, provider.as_session());
  REQUIRE(parsed_full);
  CHECK_EQUAL(parsed_full->expressions.size(), size_t{1});
  CHECK_EQUAL(parsed_full->bytes_consumed, full_source.size());
  CHECK(not parsed_full->has_error);
  CHECK(try_as<ast::record>(parsed_full->expressions[0]));
}

TEST("tql2 parser: expression stream keeps parsed prefix on hard suffix "
     "error") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto source = std::string_view{"{x:1}\n{x:2}\n}"};
  auto parsed = parse_expression_stream_with_bad_diagnostics(
    source, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{2});
  CHECK_EQUAL(parsed->bytes_consumed,
              std::string_view{"{x:1}\n{x:2}\n"}.size());
  CHECK(parsed->has_error);
  CHECK(try_as<ast::record>(parsed->expressions[0]));
  CHECK(try_as<ast::record>(parsed->expressions[1]));
  CHECK(not dh.empty());
}

TEST("tql2 parser: take expression keyword") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto parsed = parse_assignment_with_bad_diagnostics(
    "new_field = take old_field", provider.as_session());
  REQUIRE(parsed);
  auto* unary = try_as<ast::unary_expr>(parsed->right);
  REQUIRE(unary);
  CHECK_EQUAL(unary->op.inner, ast::unary_op::take);
  CHECK(dh.empty());
}

TEST("tql2 parser: move expression keyword is deprecated") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto parsed = parse_assignment_with_bad_diagnostics(
    "new_field = move old_field", provider.as_session());
  REQUIRE(parsed);
  auto* unary = try_as<ast::unary_expr>(parsed->right);
  REQUIRE(unary);
  CHECK_EQUAL(unary->op.inner, ast::unary_op::move);
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].severity, severity::warning);
  CHECK_EQUAL(diags[0].message, "`move` as an expression keyword is "
                                "deprecated; use `take` instead");
}

TEST("tql2 parser: move operator is not deprecated") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto parsed
    = parse_pipeline_with_bad_diagnostics("move x=foo", provider.as_session());
  REQUIRE(parsed);
  CHECK(dh.empty());
}

TEST("tql2 parser: take remains usable in identifier contexts") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto invocation
    = parse_pipeline_with_bad_diagnostics("take foo", provider.as_session());
  REQUIRE(invocation);
  auto access = parse_assignment_with_bad_diagnostics(
    "x = foo.take", provider.as_session());
  REQUIRE(access);
  auto field
    = parse_assignment_with_bad_diagnostics("take = 1", provider.as_session());
  REQUIRE(field);
  CHECK(dh.empty());
}

TEST("tql2 parser: deep left-associated or location") {
  auto expr = ast::expression{ast::constant{false, location{0, 1}}};
  auto end = size_t{1};
  for (auto i = size_t{1}; i < 70; ++i) {
    auto begin = i * 2;
    end = begin + 1;
    expr = ast::expression{ast::binary_expr{
      std::move(expr),
      located{ast::binary_op::or_, location{begin - 1, begin}},
      ast::expression{ast::constant{false, location{begin, end}}},
    }};
  }
  auto location = expr.get_location();
  CHECK(location);
  CHECK_EQUAL(location.begin, size_t{0});
  CHECK_EQUAL(location.end, end);
}
