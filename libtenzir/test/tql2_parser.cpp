//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/source.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"

#include <sstream>
#include <string>
#include <string_view>

using namespace tenzir;

TEST("tql2 tokenizer: ranges and spread dots") {
  auto tokens = tokenize_permissive("200..299 ... ..");
  REQUIRE_EQUAL(tokens.size(), size_t{7});
  CHECK_EQUAL(tokens[0].kind, token_kind::scalar);
  CHECK_EQUAL(tokens[0].end, size_t{3});
  CHECK_EQUAL(tokens[1].kind, token_kind::dot_dot);
  CHECK_EQUAL(tokens[1].end, size_t{5});
  CHECK_EQUAL(tokens[2].kind, token_kind::scalar);
  CHECK_EQUAL(tokens[2].end, size_t{8});
  CHECK_EQUAL(tokens[3].kind, token_kind::whitespace);
  CHECK_EQUAL(tokens[4].kind, token_kind::dot_dot_dot);
  CHECK_EQUAL(tokens[4].end, size_t{12});
  CHECK_EQUAL(tokens[5].kind, token_kind::whitespace);
  CHECK_EQUAL(tokens[6].kind, token_kind::dot_dot);
  CHECK_EQUAL(tokens[6].end, size_t{15});
}

TEST("tql2 tokenizer: source-aware token errors use source id") {
  auto source = Source::new_source("`", "<input>", true);
  auto map = SourceMap{};
  map.add_source(source);
  auto stream = std::stringstream{};
  auto printer = make_diagnostic_printer(map, color_diagnostics::no, stream);
  auto provider = session_provider::make(*printer);
  auto tokens = tokenize_permissive(source->text);
  auto result = verify_tokens(tokens, *source, provider.as_session());
  CHECK(not result);
  auto rendered = stream.str();
  CHECK(rendered.find("--> <input>:1:1") != std::string::npos);
  CHECK(rendered.find("1 | `") != std::string::npos);
}

TEST("tql2 tokenizer: location override is exact") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto tokens = tokenize_permissive("`");
  auto override = location{42, 99, 7, 3};
  auto result = verify_tokens(tokens, override, provider.as_session());
  CHECK(not result);
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  REQUIRE_EQUAL(diagnostics[0].annotations.size(), size_t{1});
  CHECK_EQUAL(diagnostics[0].annotations[0].source, override);
}

TEST("tql2 parser: bad type diagnostics use exact location override") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto override = location{12, 34, 5, 6};
  auto parsed = parse_type_def_with_location_override("foo bar", override,
                                                      provider.as_session());
  CHECK(not parsed);
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  REQUIRE_EQUAL(diagnostics[0].annotations.size(), size_t{1});
  CHECK_EQUAL(diagnostics[0].annotations[0].source, override);
}

TEST("tql2 parser: AST locations use exact location override") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto override = location{12, 34, 5, 6};
  auto parsed = parse_assignment_with_location_override(
    "$event.foo = 1", override, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->get_location(), override);
}

TEST("tql2 parser: dollar variable selector paths") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto parse = [&](std::string_view source) {
    auto assignment = parse_assignment_with_location_override(
      source, location::unknown, provider.as_session());
    REQUIRE(assignment);
    return assignment->left;
  };
  auto dotted = parse("$event.foo = 1");
  auto* dotted_access = try_as<ast::field_access>(dotted);
  REQUIRE(dotted_access);
  auto* dotted_root = try_as<ast::dollar_var>(dotted_access->left);
  REQUIRE(dotted_root);
  CHECK_EQUAL(dotted_root->id.name, "$event");
  CHECK_EQUAL(dotted_access->name.name, "foo");
  auto indexed = parse(R"($event["foo"].bar = 1)");
  auto* indexed_access = try_as<ast::field_access>(indexed);
  REQUIRE(indexed_access);
  auto* index = try_as<ast::index_expr>(indexed_access->left);
  REQUIRE(index);
  auto* indexed_root = try_as<ast::dollar_var>(index->expr);
  REQUIRE(indexed_root);
  CHECK_EQUAL(indexed_root->id.name, "$event");
  CHECK_EQUAL(indexed_access->name.name, "bar");
}

TEST("tql2 parser: expression stream parses multiple records") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto source = std::string_view{"{x:1}\n{x:2}"};
  auto parsed = parse_expression_stream_with_location_override(
    source, location::unknown, provider.as_session());
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
  auto parsed = parse_expression_stream_with_location_override(
    source, location::unknown, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{3});
  CHECK_EQUAL(parsed->bytes_consumed, source.size());
}

TEST("tql2 parser: expression stream leaves incomplete trailing suffix") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto source = std::string_view{"{x:1}\n{x:"};
  auto parsed = parse_expression_stream_with_location_override(
    source, location::unknown, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{1});
  CHECK_EQUAL(source.substr(parsed->bytes_consumed), std::string_view{"{x:"});
}

TEST("tql2 parser: expression stream accepts partial UTF-8 suffix") {
  auto first_chunk = std::string{R"({msg: ")"};
  first_chunk.push_back(static_cast<char>(0xc3));
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto parsed = parse_expression_stream_with_location_override(
    first_chunk, location::unknown, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{0});
  CHECK_EQUAL(parsed->bytes_consumed, size_t{0});
  CHECK(not parsed->has_error);
  CHECK(dh.empty());

  first_chunk.push_back(static_cast<char>(0xa4));
  first_chunk += "\"}";
  auto full_source = std::string_view{first_chunk};
  auto parsed_full = parse_expression_stream_with_location_override(
    full_source, location::unknown, provider.as_session());
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
  auto parsed = parse_expression_stream_with_location_override(
    source, location::unknown, provider.as_session());
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->expressions.size(), size_t{2});
  CHECK_EQUAL(parsed->bytes_consumed,
              std::string_view{"{x:1}\n{x:2}\n"}.size());
  CHECK(parsed->has_error);
  CHECK(try_as<ast::record>(parsed->expressions[0]));
  CHECK(try_as<ast::record>(parsed->expressions[1]));
  CHECK(not dh.empty());
}

TEST("tql2 parser: deep left-associated or location") {
  auto expr = ast::expression{ast::constant{false, location{0, 1}}};
  auto end = uint32_t{1};
  for (auto i = uint32_t{1}; i < 70; ++i) {
    auto begin = i * 2;
    end = begin + 1;
    expr = ast::expression{ast::binary_expr{
      std::move(expr),
      ast::binary_op::or_,
      ast::expression{ast::constant{false, location{begin, end}}},
    }};
  }
  auto location = expr.get_location();
  CHECK(location);
  CHECK_EQUAL(location.begin, uint32_t{0});
  CHECK_EQUAL(location.end, end);
}
