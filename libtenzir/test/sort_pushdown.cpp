//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/serialize.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"

#include <caf/binary_deserializer.hpp>

using namespace tenzir;

namespace {

auto expression_from(std::string_view text) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto expr = parse_expression_with_location_override(text, location::unknown,
                                                      provider.as_session());
  REQUIRE(expr);
  return std::move(*expr);
}

auto projection() -> Option<ir::OptimizeProjection> {
  auto field = ast::field_path::try_from(expression_from("id"));
  REQUIRE(field);
  return ir::OptimizeProjection{std::move(*field)};
}

auto compile_sort(base_ctx ctx, std::string_view text
                                = "sort nested.key, -other") -> ir::pipeline {
  auto provider = session_provider::make(ctx);
  auto ast = parse_pipeline_with_location_override(text, location::unknown,
                                                   provider.as_session());
  REQUIRE(ast);
  auto compiled = std::move(*ast).compile(compile_ctx::make_root(ctx));
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

} // namespace

TEST("sort moves every filter before selection and retains key dependencies") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto optimized
    = compile_sort(ctx).optimize({.filter = {expression_from("keep == true")},
                                  .order = EventOrder::ordered,
                                  .limit = uint64_t{2},
                                  .projection = projection()},
                                 {});
  CHECK_EQUAL(optimized.filter.size(), 1u);
  CHECK(not optimized.limit);
  REQUIRE(optimized.projection);
  auto paths = std::vector<std::string>{};
  for (auto const& field : *optimized.projection) {
    paths.push_back(field.path().front().id.name);
  }
  CHECK_EQUAL(paths,
              (std::vector<std::string>{"id", "keep", "nested", "other"}));
  for (auto text : {"sort", "sort this"}) {
    auto whole = compile_sort(ctx, text).optimize(
      {.filter = {}, .order = EventOrder::ordered, .projection = projection()},
      {});
    CHECK(not whole.projection);
  }
}

TEST("sort preserves its bound through repeated optimization and replanning") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = base_ctx{dh, provider.as_session().reg()};
  auto optimize = [](ir::pipeline pipe, Option<uint64_t> limit) {
    return std::move(pipe)
      .optimize({.filter = {}, .order = EventOrder::ordered, .limit = limit},
                {})
      .replacement;
  };
  auto expected = optimize(compile_sort(ctx), uint64_t{2});
  auto initial = optimize(compile_sort(ctx), uint64_t{7});
  auto bytes = serialize(initial);
  auto restored = ir::pipeline{};
  auto deserializer = caf::binary_deserializer{bytes};
  REQUIRE(deserializer.apply(restored));
  auto refined = optimize(std::move(restored), uint64_t{2});
  CHECK_EQUAL(serialize(refined), serialize(expected));
  for (auto limit : {Option<uint64_t>{}, Option<uint64_t>{10}}) {
    auto repeated = optimize(refined, limit);
    CHECK_EQUAL(serialize(repeated), serialize(expected));
    auto plan = ir::make_plan(std::move(repeated), tag_v<nova::Events>, ctx);
    REQUIRE(plan);
    CHECK_EQUAL(plan->operators.size(), 1u);
  }
  auto dropped
    = std::move(refined).optimize({.filter = {expression_from("keep == true")},
                                   .order = EventOrder::unordered,
                                   .limit = uint64_t{1},
                                   .projection = projection()},
                                  {});
  CHECK(dropped.replacement.operators.empty());
  CHECK_EQUAL(dropped.limit, uint64_t{1});
  CHECK_EQUAL(dropped.filter.size(), 1u);
  REQUIRE(dropped.projection);
  CHECK_EQUAL(dropped.projection->size(), 1u);
}
