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

TEST("tql2 parser: parses @timestamp assignment") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto parsed = parse_assignment_with_bad_diagnostics("@timestamp = foo.bar",
                                                      provider.as_session());
  REQUIRE(parsed);
  auto* meta = std::get_if<ast::meta>(&parsed->left);
  REQUIRE(meta);
  CHECK_EQUAL(meta->kind, ast::meta::timestamp);
  auto copy = ast::expression{parsed->right};
  auto path = ast::field_path::try_from(std::move(copy));
  REQUIRE(path);
  CHECK_EQUAL(path->path().size(), size_t{2});
  CHECK_EQUAL(path->path()[0].id.name, "foo");
  CHECK_EQUAL(path->path()[1].id.name, "bar");
}
