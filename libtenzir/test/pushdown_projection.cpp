//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arc.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/json_parser.hpp"
#include "tenzir/nova/projection_tree.hpp"
#include "tenzir/nova/stringify.hpp"
#include "tenzir/pushdown/read.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"

#include <fmt/format.h>

#include <cstdint>
#include <initializer_list>
#include <random>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;

namespace {

auto projection_from(std::initializer_list<std::string_view> fields)
  -> ir::OptimizeProjection {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto session = provider.as_session();
  auto result = ir::OptimizeProjection{};
  for (auto field : fields) {
    auto expr = parse_expression_with_location_override(
      field, location::unknown, session);
    REQUIRE(expr);
    auto path = ast::field_path::try_from(std::move(*expr));
    REQUIRE(path);
    result.push_back(std::move(*path));
  }
  return result;
}

auto tree_from(std::initializer_list<std::string_view> fields)
  -> Arc<nova::ProjectionTree const> {
  auto tree = pushdown::make_projection_tree(projection_from(fields));
  REQUIRE(tree);
  return std::move(*tree);
}

auto parse_lines(std::vector<std::string> const& lines,
                 nova::JsonParser::Settings settings) -> nova::Events {
  auto dh = null_diagnostic_handler{};
  auto parser = nova::JsonParser::make(std::move(settings), dh);
  REQUIRE(parser);
  for (auto line : lines) {
    parser->parse(line);
  }
  parser->flush();
  auto ready = parser->take_ready();
  REQUIRE_EQUAL(ready.size(), size_t{1});
  return std::move(ready[0]);
}

/// Evaluates `expr_text` on `events` and prints each active row as JSON, plus
/// any diagnostics emitted during evaluation.
auto eval_and_print(nova::Events const& events, std::string_view expr_text)
  -> std::pair<std::vector<std::string>, std::vector<std::string>> {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto session = provider.as_session();
  auto expr = parse_expression_with_location_override(
    expr_text, location::unknown, session);
  REQUIRE(expr);
  auto eval_dh = collecting_diagnostic_handler{};
  auto evaluator
    = test::make_evaluator(std::move(*expr), eval_dh, session.reg());
  REQUIRE(evaluator);
  auto values = evaluator->eval(events, nova::EvalCtx{eval_dh});
  auto opts = json_printer_options{.oneline = true};
  auto strings = nova::stringify(values, events.mask, opts);
  auto rendered = std::vector<std::string>{};
  rendered.reserve(static_cast<size_t>(events.length()));
  for (auto row = nova::storage::Index{0}; row < events.length(); ++row) {
    rendered.emplace_back(*strings.get(row));
  }
  auto diags = std::vector<std::string>{};
  for (auto const& d : std::move(eval_dh).collect()) {
    diags.push_back(d.message);
  }
  return {std::move(rendered), std::move(diags)};
}

/// Generates a random JSON value using deterministic PRNG state.
auto random_json_value(std::mt19937_64& rng, int depth) -> std::string {
  auto pick = [&](size_t n) -> size_t {
    return rng() % n;
  };
  constexpr std::string_view scalars[] = {
    "null",
    "true",
    "false",
    "0",
    "42",
    "-7",
    "3.5",
    R"("hello")",
    R"("10.0.0.1")",
    R"("2026-01-02T03:04:05Z")",
    R"("80")",
    R"("a\"b")",
  };
  if (depth >= 2) {
    return std::string{scalars[pick(std::size(scalars))]};
  }
  auto const kind = pick(10);
  if (kind < 5) {
    return std::string{scalars[pick(std::size(scalars))]};
  }
  if (kind < 7) {
    // Array of scalars or objects (tests that arrays under projected paths are
    // never projected through their elements).
    auto count = pick(4);
    auto result = std::string{"["};
    for (auto i = size_t{0}; i < count; ++i) {
      if (i > 0) {
        result += ',';
      }
      result += random_json_value(rng, depth + 1);
    }
    result += ']';
    return result;
  }
  // Nested object with plain, dotted, escaped, empty-segment, and duplicate
  // keys.
  constexpr std::string_view keys[] = {
    "a",   "b", "c",    "d",        "a.b", "a.c", "a.b.c",
    "b.c", "x", "kind", "a\\u0062", ".b",  "a.",  "a..b",
  };
  auto count = pick(6);
  auto result = std::string{"{"};
  for (auto i = size_t{0}; i < count; ++i) {
    if (i > 0) {
      result += ',';
    }
    auto const key = keys[pick(std::size(keys))];
    result += '"';
    result += key;
    result += R"(":)";
    result += random_json_value(rng, depth + 1);
  }
  result += '}';
  return result;
}

auto random_json_object(std::mt19937_64& rng) -> std::string {
  constexpr std::string_view keys[] = {
    "a",   "b", "c",    "d",        "a.b", "a.c", "a.b.c", "a.d",
    "b.c", "x", "kind", "a\\u0062", ".b",  "a.",  "a..b",  "kind.sub",
  };
  auto const count = rng() % 8;
  auto result = std::string{"{"};
  for (auto i = size_t{0}; i < count; ++i) {
    if (i > 0) {
      result += ',';
    }
    auto const key = keys[rng() % std::size(keys)];
    result += '"';
    result += key;
    result += R"(":)";
    result += random_json_value(rng, 0);
  }
  result += '}';
  return result;
}

} // namespace

TEST("projection tree handles empty, wildcard, subsumption, and nesting") {
  auto empty = tree_from({});
  CHECK(empty->root().is_empty());
  CHECK(not empty->root().is_all());
  CHECK(empty->root().lookup("a") == nullptr);
  // Whole-event projection (`this`) or unrestricted `None` returns no tree.
  CHECK(not pushdown::make_projection_tree(projection_from({"this"})));
  CHECK(not pushdown::make_projection_tree(None{}));
  auto wildcard = nova::ProjectionTree{};
  wildcard.add_path({});
  CHECK(wildcard.root().is_all());
  CHECK(wildcard.root().lookup("anything") == &wildcard.root());
  // Adding a broader path subsumes narrower paths in either order.
  auto narrow_then_broad = tree_from({"a.b.c", "a"});
  auto const* a = narrow_then_broad->root().lookup("a");
  REQUIRE(a != nullptr);
  CHECK(a->is_all());
  CHECK(a->lookup("anything") == a);
  auto broad_then_narrow = tree_from({"a", "a.b.c"});
  a = broad_then_narrow->root().lookup("a");
  REQUIRE(a != nullptr);
  CHECK(a->is_all());
  CHECK(a->lookup("anything") == a);
  // Disjoint nested paths stay selective.
  auto nested = tree_from({"a.b", "a.c.d", "x"});
  a = nested->root().lookup("a");
  REQUIRE(a != nullptr);
  CHECK(not a->is_all());
  CHECK(a->lookup("b") != nullptr);
  CHECK(a->lookup("b")->is_all());
  CHECK(a->lookup("z") == nullptr);
  auto const* c = a->lookup("c");
  REQUIRE(c != nullptr);
  CHECK(not c->is_all());
  CHECK(c->lookup("d") != nullptr);
  CHECK(c->lookup("d")->is_all());
  CHECK(nested->root().lookup("x") != nullptr);
  CHECK(nested->root().lookup("x")->is_all());
  CHECK(nested->root().lookup("y") == nullptr);
}

TEST("schema pruning retains nested paths, attributes, and non-record "
     "interiors") {
  auto ts_with_unit = type{time_type{}, {{"unit", "s"}}};
  auto schema = type{
    "test.event",
    record_type{
      {"ts", ts_with_unit},
      {"unused", string_type{}},
      {"nested",
       record_type{
         {"keep", int64_type{}},
         {"drop", string_type{}},
         {"scalar", uint64_type{}},
       }},
      {"list_field",
       list_type{record_type{{"a", int64_type{}}, {"b", string_type{}}}}},
    },
  };
  auto tree
    = tree_from({"ts", "nested.keep", "nested.scalar.sub", "list_field.a"});
  auto pruned = tree->prune_schema(schema);
  CHECK_EQUAL(pruned.name(), "test.event");
  auto const& root = as<record_type>(pruned);
  REQUIRE_EQUAL(root.num_fields(), size_t{3});
  // Attributes on kept leaf types survive pruning.
  auto ts_idx = root.resolve_field("ts");
  REQUIRE(ts_idx);
  auto const ts_type = root.field(*ts_idx).type;
  CHECK_EQUAL(ts_type.attribute("unit"), Option<std::string_view>{"s"});
  CHECK(not root.resolve_field("unused"));
  // Nested records prune dropped siblings and keep non-record interior fields.
  auto nested_idx = root.resolve_field("nested");
  REQUIRE(nested_idx);
  auto const& nested = as<record_type>(root.field(*nested_idx).type);
  REQUIRE_EQUAL(nested.num_fields(), size_t{2});
  CHECK(nested.resolve_field("keep").has_value());
  CHECK(not nested.resolve_field("drop").has_value());
  auto scalar_idx = nested.resolve_field("scalar");
  REQUIRE(scalar_idx);
  CHECK(is<uint64_type>(nested.field(*scalar_idx).type));
  // Non-record types (lists) stay intact even when a child segment is named.
  auto list_idx = root.resolve_field("list_field");
  REQUIRE(list_idx);
  CHECK_EQUAL(
    as<record_type>(as<list_type>(root.field(*list_idx).type).value_type())
      .num_fields(),
    size_t{2});
  // Empty projection yields an empty named record schema.
  auto empty = tree_from({});
  auto pruned_empty = empty->prune_schema(schema);
  CHECK_EQUAL(pruned_empty.name(), "test.event");
  CHECK_EQUAL(as<record_type>(pruned_empty).num_fields(), size_t{0});
}

TEST("selective JSON decoding skips unselected malformed values while "
     "preserving framing") {
  auto settings = nova::JsonParser::Settings{};
  settings.builder.projection = tree_from({"id", "nested.x", "y"});
  auto dh = collecting_diagnostic_handler{};
  auto parser = nova::JsonParser::make(std::move(settings), dh);
  REQUIRE(parser);
  // Malformed scalars and invalid inner syntax (such as trailing commas) inside
  // unselected fields are skipped over simdjson's structural tape.
  auto line = std::string{
    R"({"id": 42, "bad_scalar": 123abc, "nested": {"x": 7, "bad_sibling": truX}, "bad_struct": {"k": 1, }, "y": 9})"};
  parser->parse(line);
  parser->flush();
  auto ready = parser->take_ready();
  REQUIRE_EQUAL(ready.size(), size_t{1});
  CHECK(std::move(dh).collect().empty());
  auto [ids, id_diags] = eval_and_print(ready[0], "id");
  CHECK_EQUAL(ids, (std::vector<std::string>{"42"}));
  CHECK(id_diags.empty());
  auto [xs, x_diags] = eval_and_print(ready[0], "nested.x");
  CHECK_EQUAL(xs, (std::vector<std::string>{"7"}));
  CHECK(x_diags.empty());
  auto [ys, y_diags] = eval_and_print(ready[0], "y");
  CHECK_EQUAL(ys, (std::vector<std::string>{"9"}));
  CHECK(y_diags.empty());
  // Unclosed objects still fail framing even under a projection.
  auto unclosed_dh = collecting_diagnostic_handler{};
  auto unclosed_settings = nova::JsonParser::Settings{};
  unclosed_settings.builder.projection = tree_from({"id"});
  auto unclosed_parser
    = nova::JsonParser::make(std::move(unclosed_settings), unclosed_dh);
  REQUIRE(unclosed_parser);
  auto unclosed = std::string{R"({"id": 1, "bad": {"k": 1)"};
  unclosed_parser->parse(unclosed);
  unclosed_parser->flush();
  CHECK(unclosed_parser->take_ready().empty());
  CHECK(not std::move(unclosed_dh).collect().empty());
  // Empty projection skips all fields while still detecting trailing content in
  // single-document mode.
  auto doc_settings = nova::JsonParser::Settings{};
  doc_settings.builder.projection = tree_from({});
  auto doc_dh = collecting_diagnostic_handler{};
  auto doc_parser = nova::JsonParser::make(std::move(doc_settings), doc_dh);
  REQUIRE(doc_parser);
  auto doc_ok = doc_parser->parse_document(R"({"a": 123abc, "b": [1, 2]})");
  REQUIRE(doc_ok);
  CHECK_EQUAL(doc_ok->length(), 1);
  CHECK(doc_dh.empty());
  auto doc_trailing = doc_parser->parse_document(R"({"a": 1} trailing)");
  CHECK(not doc_trailing);
  CHECK(not std::move(doc_dh).collect().empty());
}

TEST("selective JSON decoding supports duplicate-key decoding options") {
  // first_duplicate_wins keeps the first value of projected keys and skips
  // unselected keys without aborting.
  {
    auto settings = nova::JsonParser::Settings{};
    settings.decoding.first_duplicate_wins = true;
    settings.builder.projection = tree_from({"id", "nested.x"});
    auto dh = collecting_diagnostic_handler{};
    auto parser = nova::JsonParser::make(std::move(settings), dh);
    REQUIRE(parser);
    auto doc = parser->parse_document(
      R"({"id": 1, "id": 2, "drop": 10, "drop": 20, "nested": {"x": 3, "x": 4, "y": 5}})");
    REQUIRE(doc);
    CHECK(std::move(dh).collect().empty());
    CHECK_EQUAL(eval_and_print(*doc, "id").first,
                (std::vector<std::string>{"1"}));
    CHECK_EQUAL(eval_and_print(*doc, "nested.x").first,
                (std::vector<std::string>{"3"}));
  }
  // reject_duplicate_keys still rejects repeated unselected keys, including
  // under an empty projection.
  for (auto const& proj : {tree_from({"id"}), tree_from({})}) {
    auto settings = nova::JsonParser::Settings{};
    settings.decoding.reject_duplicate_keys = true;
    settings.builder.projection = proj;
    auto dh = collecting_diagnostic_handler{};
    auto parser = nova::JsonParser::make(std::move(settings), dh);
    REQUIRE(parser);
    auto ok_doc = parser->parse_document(R"({"id": 1, "other": 2})");
    REQUIRE(ok_doc);
    CHECK(dh.empty());
    auto dup_doc
      = parser->parse_document(R"({"id": 1, "other": 2, "other": 3})");
    CHECK(not dup_doc);
    auto diags = std::move(dh).collect();
    REQUIRE_EQUAL(diags.size(), size_t{1});
    CHECK_EQUAL(diags[0].message, "duplicate JSON object key");
  }
}

TEST("selective JSON decoding matches full decoding across random inputs and "
     "policies") {
  auto rng = std::mt19937_64{0x5eed1043ULL};
  auto lines = std::vector<std::string>{};
  constexpr auto num_rows = size_t{120};
  lines.reserve(num_rows);
  for (auto i = size_t{0}; i < num_rows; ++i) {
    lines.push_back(random_json_object(rng));
  }
  auto const projections = std::vector<std::vector<std::string_view>>{
    {},          {"a"},          {"a.b"},
    {"a.b.c"},   {"a.b", "b.c"}, {"a.b", "a.c", "x"},
    {"ab", "d"}, {"a", "a.b"},   {"kind", "a.b"},
  };
  auto const separators = std::vector<std::string>{"", "."};
  auto const selector_fields = std::vector<std::string>{"", "kind", "kind.sub"};
  for (auto const& separator : separators) {
    for (auto raw : {false, true}) {
      for (auto const& selector_field : selector_fields) {
        auto base_settings = nova::JsonParser::Settings{};
        base_settings.batch_size = static_cast<int64_t>(num_rows + 1);
        base_settings.builder.raw = raw;
        base_settings.builder.unflatten_separator = separator;
        if (not selector_field.empty()) {
          base_settings.builder.policy = nova::EventBuilder::SelectorPolicy{
            selector_field, std::string{"pfx"}};
        }
        auto full = parse_lines(lines, base_settings);
        for (auto const& proj_fields : projections) {
          auto paths = ir::OptimizeProjection{};
          for (auto field : proj_fields) {
            auto single = projection_from({field});
            paths.push_back(std::move(single.front()));
          }
          // Do not add `selector_field` manually: `EventBuilder::make` must
          // retain it automatically whenever `SelectorPolicy` is active.
          auto tree = pushdown::make_projection_tree(std::move(paths));
          REQUIRE(tree);
          auto proj_settings = base_settings;
          proj_settings.builder.projection = std::move(*tree);
          auto selective = parse_lines(lines, proj_settings);
          REQUIRE_EQUAL(selective.length(), full.length());
          // Schema names (`@name`) must match on every row.
          for (auto row = nova::storage::Index{0}; row < full.length(); ++row) {
            CHECK_EQUAL(*selective.meta.name.get(row),
                        *full.meta.name.get(row));
          }
          // Every projected path must evaluate identically, including
          // diagnostics on missing fields and intermediate records.
          for (auto field : proj_fields) {
            auto [full_vals, full_diags] = eval_and_print(full, field);
            auto [sel_vals, sel_diags] = eval_and_print(selective, field);
            CHECK_EQUAL(sel_vals, full_vals);
            CHECK_EQUAL(sel_diags, full_diags);
            auto optional_expr = fmt::format("{}?", field);
            auto [full_opt, full_opt_diags]
              = eval_and_print(full, optional_expr);
            auto [sel_opt, sel_opt_diags]
              = eval_and_print(selective, optional_expr);
            CHECK_EQUAL(sel_opt, full_opt);
            CHECK_EQUAL(sel_opt_diags, full_opt_diags);
          }
        }
      }
    }
  }
}
