//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/read_pushdown.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/serialize.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"

#include <caf/binary_deserializer.hpp>

using namespace tenzir;

namespace {

auto expression_from(std::string_view text) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  auto expr
    = parse_expression_with_location_override(text, location::unknown, s);
  REQUIRE(expr);
  return std::move(*expr);
}

auto projection_from(std::string_view text) -> Option<ir::OptimizeProjection> {
  auto field = ast::field_path::try_from(expression_from(text));
  REQUIRE(field);
  auto result = ir::OptimizeProjection{};
  result.push_back(std::move(*field));
  return result;
}

auto projection_from(std::initializer_list<std::string_view> fields)
  -> Option<ir::OptimizeProjection> {
  auto result = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
  for (auto field : fields) {
    ir::add_to_projection(result, projection_from(field)->front());
  }
  return result;
}

auto projection_paths(Option<ir::OptimizeProjection> const& projection)
  -> std::vector<std::vector<std::string>> {
  REQUIRE(projection);
  auto result = std::vector<std::vector<std::string>>{};
  for (auto const& field : *projection) {
    auto path = std::vector<std::string>{};
    for (auto const& segment : field.path()) {
      path.push_back(segment.id.name);
    }
    result.push_back(std::move(path));
  }
  return result;
}

auto compile_reader(base_ctx ctx) -> ir::pipeline {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ast = parse_pipeline_with_location_override(
    "read_feather", location::unknown, provider.as_session());
  REQUIRE(ast);
  auto root = compile_ctx::make_root(ctx);
  auto compiled = std::move(*ast).compile(root);
  REQUIRE(compiled);
  auto instantiated = ir::instantiate(std::move(*compiled), ctx);
  REQUIRE(instantiated);
  return std::move(*instantiated);
}

auto serialize(auto const& value) -> caf::byte_buffer {
  auto result = caf::byte_buffer{};
  REQUIRE(detail::serialize(result, value));
  return result;
}

auto slice_from(int64_t start) -> table_slice {
  auto builder = series_builder{};
  for (auto i = start; i < start + 3; ++i) {
    builder.record().field("id").data(i);
  }
  return builder.finish_assert_one_slice("test.read");
}

} // namespace

TEST("reader projection includes filter columns and retains top-level "
     "records") {
  auto filter = ir::OptimizeFilter{};
  filter.push_back(expression_from("nested.x >= 4"));
  auto result = read_projection(projection_from("id"), filter);
  REQUIRE(result);
  CHECK_EQUAL(*result, (std::vector<std::string>{"id", "nested"}));
  result = read_projection(projection_from("nested.x"), {});
  REQUIRE(result);
  CHECK_EQUAL(*result, (std::vector<std::string>{"nested"}));
}

TEST("reader projection conservatively handles whole events and functions") {
  CHECK(not read_projection(projection_from("this"), {}));
  auto filter = ir::OptimizeFilter{};
  filter.push_back(expression_from("string(id) == \"4\""));
  CHECK(not read_projection(projection_from("id"), filter));
  CHECK(not read_projection({}, {}));
}

TEST("reader limit counts matching rows across batches") {
  auto filter = ir::OptimizeFilter{};
  filter.push_back(expression_from("id >= 4"));
  auto remaining = Option<uint64_t>{3};
  auto dh = collecting_diagnostic_handler{};
  CHECK_EQUAL(apply_read_pushdown(slice_from(0), filter, remaining, dh).rows(),
              0u);
  CHECK_EQUAL(*remaining, 3u);
  CHECK_EQUAL(apply_read_pushdown(slice_from(3), filter, remaining, dh).rows(),
              2u);
  CHECK_EQUAL(*remaining, 1u);
  CHECK_EQUAL(apply_read_pushdown(slice_from(6), filter, remaining, dh).rows(),
              1u);
  CHECK_EQUAL(*remaining, 0u);
  CHECK_EQUAL(apply_read_pushdown(slice_from(9), filter, remaining, dh).rows(),
              0u);
}

TEST("projection intersection treats unrestricted requests as the identity") {
  auto projection = Option<ir::OptimizeProjection>{};
  ir::intersect_projection(projection, None{});
  CHECK(not projection);
  ir::intersect_projection(projection, projection_from("id"));
  CHECK_EQUAL(projection_paths(projection),
              projection_paths(projection_from("id")));
  ir::intersect_projection(projection, None{});
  CHECK_EQUAL(projection_paths(projection),
              projection_paths(projection_from("id")));
  ir::intersect_projection(projection, projection_from("id"));
  CHECK_EQUAL(projection_paths(projection),
              projection_paths(projection_from("id")));
}

TEST("projection intersection narrows fields and preserves empty projections") {
  auto projection = projection_from({"id", "name"});
  ir::intersect_projection(projection, projection_from("id"));
  CHECK_EQUAL(projection_paths(projection),
              projection_paths(projection_from("id")));
  ir::intersect_projection(projection, projection_from("name"));
  REQUIRE(projection);
  CHECK(projection->empty());
  ir::intersect_projection(projection, None{});
  REQUIRE(projection);
  CHECK(projection->empty());
  projection = projection_from("id");
  ir::intersect_projection(projection, ir::OptimizeProjection{});
  REQUIRE(projection);
  CHECK(projection->empty());
}

TEST("projection intersection retains the narrower nested path in either "
     "order") {
  auto broad = projection_from({"nested", "id"});
  auto narrow = projection_from({"nested.x", "nested.y", "unused"});
  auto expected = projection_paths(projection_from({"nested.x", "nested.y"}));
  auto result = broad;
  ir::intersect_projection(result, narrow);
  CHECK_EQUAL(projection_paths(result), expected);
  result = narrow;
  ir::intersect_projection(result, broad);
  CHECK_EQUAL(projection_paths(result), expected);
  ir::intersect_projection(result, narrow);
  CHECK_EQUAL(projection_paths(result), expected);
  result = projection_from("nested.x");
  ir::intersect_projection(result, projection_from("nested.y"));
  REQUIRE(result);
  CHECK(result->empty());
}

TEST("projection intersection compares path segments rather than dotted "
     "names") {
  auto projection = projection_from("this[\"nested.x\"]");
  ir::intersect_projection(projection, projection_from("nested.x"));
  REQUIRE(projection);
  CHECK(projection->empty());
  projection = projection_from("this");
  ir::intersect_projection(projection, projection_from("nested.x"));
  CHECK_EQUAL(projection_paths(projection),
              projection_paths(projection_from("nested.x")));
}

TEST("refined reader projection includes accumulated filter dependencies") {
  auto projection = projection_from({"id", "unused"});
  ir::intersect_projection(projection, projection_from("id"));
  auto filter = ir::OptimizeFilter{};
  filter.push_back(expression_from("nested.x >= 4"));
  filter.push_back(expression_from("other == 1"));
  auto columns = read_projection(projection, filter);
  REQUIRE(columns);
  CHECK_EQUAL(*columns, (std::vector<std::string>{"id", "nested", "other"}));
}

TEST("runtime reader planning preserves pushed hints including after "
     "serialization") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto filter = ir::OptimizeFilter{};
  filter.push_back(expression_from("nested.x >= 4"));
  auto optimized = compile_reader(ctx).optimize(
    ir::OptimizeRequest{.filter = std::move(filter),
                        .order = EventOrder::ordered,
                        .limit = uint64_t{2},
                        .projection = projection_from("id")},
    {});
  auto pipe = std::move(optimized.replacement);
  REQUIRE_EQUAL(pipe.operators.size(), 1u);
  auto expected = serialize(pipe.operators.front());
  for (auto roundtrip : {false, true}) {
    auto input = pipe;
    if (roundtrip) {
      auto bytes = serialize(input);
      input = ir::pipeline{};
      auto deserializer = caf::binary_deserializer{bytes};
      REQUIRE(deserializer.apply(input));
    }
    auto plan = ir::make_plan(std::move(input), tag_v<chunk_ptr>, ctx);
    REQUIRE(plan);
    REQUIRE_EQUAL(plan->operators.size(), 1u);
    CHECK_EQUAL(serialize(plan->operators.front().op), expected);
  }
}

TEST("repeated reader optimization accumulates all inputs across "
     "serialization") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto first = expression_from("nested.x >= 4");
  auto second = expression_from("id <= 7");
  auto expected = compile_reader(ctx).optimize(
    ir::OptimizeRequest{.filter = {first, second},
                        .order = EventOrder::unordered,
                        .limit = uint64_t{2},
                        .projection = projection_from("id")},
    {});
  auto initial = compile_reader(ctx).optimize(
    ir::OptimizeRequest{.filter = {first},
                        .order = EventOrder::unordered,
                        .limit = uint64_t{5},
                        .projection = projection_from({"id", "unused"})},
    {});
  REQUIRE(initial.filter.empty());
  auto bytes = serialize(initial.replacement);
  auto restored = ir::pipeline{};
  auto deserializer = caf::binary_deserializer{bytes};
  REQUIRE(deserializer.apply(restored));
  auto refined = std::move(restored).optimize(
    ir::OptimizeRequest{.filter = {second},
                        .order = EventOrder::ordered,
                        .limit = uint64_t{2},
                        .projection = projection_from("id")},
    {});
  REQUIRE(refined.filter.empty());
  CHECK_EQUAL(serialize(refined.replacement), serialize(expected.replacement));
  // Neither a larger limit nor an unrestricted projection undoes acceptance.
  auto final = std::move(refined.replacement)
                 .optimize(ir::OptimizeRequest{.filter = {},
                                               .order = EventOrder::ordered,
                                               .limit = uint64_t{9}},
                           {});
  CHECK_EQUAL(serialize(final.replacement), serialize(expected.replacement));
}

TEST("repeated reader optimization refines an existing projection") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto optimize
    = [](ir::pipeline pipe, Option<ir::OptimizeProjection> projection) {
        return std::move(pipe)
          .optimize(ir::OptimizeRequest{.filter = {},
                                        .order = EventOrder::ordered,
                                        .projection = std::move(projection)},
                    {})
          .replacement;
      };
  auto expected = optimize(compile_reader(ctx), projection_from("nested.x"));
  for (auto initial : {Option<ir::OptimizeProjection>{},
                       projection_from({"nested", "unused"})}) {
    auto pipe = optimize(compile_reader(ctx), std::move(initial));
    pipe = optimize(std::move(pipe), projection_from("nested.x"));
    CHECK_EQUAL(serialize(pipe), serialize(expected));
  }
}
