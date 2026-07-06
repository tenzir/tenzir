//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/executor.hpp"
#include "tenzir/base_ctx.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/parallel_plan.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"

namespace tenzir {

namespace {

/// Parses, compiles, and instantiates a TQL pipeline into IR.
auto compile_to_ir(std::string_view source) -> ir::pipeline {
  auto dh = null_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  auto ast
    = parse_pipeline_with_location_override(source, location::unknown, s);
  REQUIRE(ast);
  auto reg = global_registry();
  auto b_ctx = base_ctx{dh, *reg};
  auto root = compile_ctx::make_root(b_ctx);
  auto ir = std::move(*ast).compile(root);
  REQUIRE(ir);
  auto sub_ctx = substitute_ctx{b_ctx, nullptr};
  auto subst = ir->substitute(sub_ctx, true);
  REQUIRE(subst);
  return std::move(*ir);
}

auto opts(size_t degree) -> plan_options {
  return plan_options{
    .degree = degree,
    .min_region_size = 1,
    .allow_reordering = true,
    .checkpointing = false,
  };
}

/// Compiles `source` (an events-to-events pipeline), plans it with `degree`,
/// and spawns the staged chains.
auto stage(std::string_view source, size_t degree) -> StagedChains {
  auto ir = compile_to_ir(source);
  auto plan = plan_stages(ir, opts(degree));
  auto dh = null_diagnostic_handler{};
  auto staged = spawn_staged(std::move(ir), plan, tag_v<table_slice>, dh);
  REQUIRE(staged);
  return std::move(*staged);
}

} // namespace

TEST("a serial plan collapses into a single stage") {
  // All-Single grouping already yields one plan stage; the interesting case
  // is mixed classes at degree 1: Single, Any, Single must still merge.
  auto staged
    = stage("summarize n=sum(x)\nwhere n != 1\nsummarize m=sum(n)", 1);
  REQUIRE_EQUAL(staged.stages.size(), size_t{1});
  CHECK_EQUAL(staged.stages[0].lanes.size(), size_t{1});
  CHECK_EQUAL(staged.stages[0].lanes[0].size(), size_t{3});
  CHECK_EQUAL(staged.num_operators(), size_t{3});
}

TEST("a gated plan collapses into a single stage") {
  // The Any region is widenable but the degree gate pulls it back to serial;
  // the layout must still be a single stage.
  auto ir
    = compile_to_ir("summarize n=sum(x)\nwhere n != 1\nsummarize m=sum(n)");
  auto gated = opts(4);
  gated.min_region_size = 10;
  auto plan = plan_stages(ir, gated);
  CHECK(not plan.parallelized());
  auto dh = null_diagnostic_handler{};
  auto staged = spawn_staged(std::move(ir), plan, tag_v<table_slice>, dh);
  REQUIRE(staged);
  REQUIRE_EQUAL(staged->stages.size(), size_t{1});
  CHECK_EQUAL(staged->stages[0].lanes.size(), size_t{1});
}

TEST("a widened region splits off serial neighbors") {
  auto staged = stage(
    "summarize n=sum(x)\nwhere n != 1\nn = n + 1\nsummarize m=sum(n)", 4);
  REQUIRE_EQUAL(staged.stages.size(), size_t{3});
  // Serial head.
  CHECK_EQUAL(staged.stages[0].lanes.size(), size_t{1});
  CHECK_EQUAL(staged.stages[0].lanes[0].size(), size_t{1});
  // Widened middle: four independently spawned lanes of two operators.
  CHECK_EQUAL(staged.stages[1].lanes.size(), size_t{4});
  for (const auto& lane : staged.stages[1].lanes) {
    CHECK_EQUAL(lane.size(), size_t{2});
  }
  // Serial tail.
  CHECK_EQUAL(staged.stages[2].lanes.size(), size_t{1});
}

TEST("a fully stateless pipeline widens into a single stage") {
  auto staged = stage("where x != 1\nx = x + 1", 4);
  REQUIRE_EQUAL(staged.stages.size(), size_t{1});
  CHECK_EQUAL(staged.stages[0].lanes.size(), size_t{4});
}

} // namespace tenzir
