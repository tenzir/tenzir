//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/parallel_plan.hpp"

#include "tenzir/base_ctx.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"

namespace tenzir {

namespace {

auto any() -> Distribution {
  return AnyDistribution{};
}
auto single() -> Distribution {
  return SingleDistribution{};
}

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

// A default set of options that would parallelize an `Any` region.
auto parallel_opts(size_t degree = 4) -> plan_options {
  return plan_options{
    .degree = degree,
    .min_region_size = 1,
    .allow_reordering = true,
    .checkpointing = false,
  };
}

} // namespace

TEST("empty pipeline yields no stages") {
  auto plan = plan_stages(std::span<const Distribution>{}, parallel_opts());
  CHECK(plan.stages.empty());
  CHECK(not plan.parallelized());
}

TEST("a single Any region becomes one parallel stage") {
  auto reqs = std::vector<Distribution>{any(), any(), any()};
  auto plan = plan_stages(reqs, parallel_opts(4));
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].begin, size_t{0});
  CHECK_EQUAL(plan.stages[0].end, size_t{3});
  CHECK_EQUAL(plan.stages[0].degree, size_t{4});
  CHECK(is<AnyDistribution>(plan.stages[0].distribution));
  CHECK(plan.parallelized());
}

TEST("Single operators stay serial and split the region") {
  // Any, Any, Single, Any
  auto reqs = std::vector<Distribution>{any(), any(), single(), any()};
  auto plan = plan_stages(reqs, parallel_opts(4));
  REQUIRE_EQUAL(plan.stages.size(), size_t{3});
  // First Any region [0,2) is parallel.
  CHECK_EQUAL(plan.stages[0].begin, size_t{0});
  CHECK_EQUAL(plan.stages[0].end, size_t{2});
  CHECK_EQUAL(plan.stages[0].degree, size_t{4});
  // Single [2,3) is serial.
  CHECK_EQUAL(plan.stages[1].begin, size_t{2});
  CHECK_EQUAL(plan.stages[1].end, size_t{3});
  CHECK_EQUAL(plan.stages[1].degree, size_t{1});
  CHECK(is<SingleDistribution>(plan.stages[1].distribution));
  // Trailing Any [3,4) is a size-1 parallel region.
  CHECK_EQUAL(plan.stages[2].begin, size_t{3});
  CHECK_EQUAL(plan.stages[2].end, size_t{4});
  CHECK_EQUAL(plan.stages[2].degree, size_t{4});
}

TEST("consecutive Single operators merge into one serial stage") {
  auto reqs = std::vector<Distribution>{single(), single(), single()};
  auto plan = plan_stages(reqs, parallel_opts(4));
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{1});
  CHECK(not plan.parallelized());
}

TEST("degree of 1 disables widening") {
  auto reqs = std::vector<Distribution>{any(), any()};
  auto plan = plan_stages(reqs, parallel_opts(1));
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{1});
  CHECK(not plan.parallelized());
}

TEST("checkpointing guard forces everything serial") {
  auto reqs = std::vector<Distribution>{any(), any(), any()};
  auto opts = parallel_opts(4);
  opts.checkpointing = true;
  auto plan = plan_stages(reqs, opts);
  // Grouping is unchanged, but no stage is widened.
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{1});
  CHECK(not plan.parallelized());
}

TEST("ordering gate disables widening") {
  auto reqs = std::vector<Distribution>{any(), any(), any()};
  auto opts = parallel_opts(4);
  opts.allow_reordering = false;
  auto plan = plan_stages(reqs, opts);
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{1});
}

TEST("cost heuristic keeps small regions serial") {
  // Region of 2 Any operators, but min_region_size is 3.
  auto reqs = std::vector<Distribution>{any(), any()};
  auto opts = parallel_opts(4);
  opts.min_region_size = 3;
  auto plan = plan_stages(reqs, opts);
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{1});
}

TEST("cost heuristic widens regions at or above the threshold") {
  auto reqs = std::vector<Distribution>{any(), any(), any()};
  auto opts = parallel_opts(4);
  opts.min_region_size = 3;
  auto plan = plan_stages(reqs, opts);
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{4});
}

TEST("hash regions stay serial in phase 1") {
  auto reqs = std::vector<Distribution>{HashDistribution{}, HashDistribution{}};
  auto plan = plan_stages(reqs, parallel_opts(4));
  // Two empty-key hashes group together but do not widen.
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{1});
  CHECK(is<HashDistribution>(plan.stages[0].distribution));
}

TEST("stateless operators are declared Any") {
  auto pipe = compile_to_ir("where x > 0 | set y = 1 | drop z | select a");
  REQUIRE_EQUAL(pipe.operators.size(), size_t{4});
  for (const auto& op : pipe.operators) {
    CHECK(is<AnyDistribution>(op->required_distribution()));
  }
  auto plan = plan_stages(pipe, parallel_opts(4));
  REQUIRE_EQUAL(plan.stages.size(), size_t{1});
  CHECK_EQUAL(plan.stages[0].degree, size_t{4});
}

TEST("summarize is single by default") {
  auto pipe = compile_to_ir("summarize n=count()");
  REQUIRE_EQUAL(pipe.operators.size(), size_t{1});
  CHECK(is<SingleDistribution>(pipe.operators[0]->required_distribution()));
}

TEST("group declares a hash distribution over its key") {
  auto pipe = compile_to_ir("group x { head }");
  REQUIRE_EQUAL(pipe.operators.size(), size_t{1});
  CHECK(is<HashDistribution>(pipe.operators[0]->required_distribution()));
}

TEST("if with stateless branches derives Any") {
  auto pipe = compile_to_ir("if x > 0 { set y = 1 } else { drop z }");
  REQUIRE_EQUAL(pipe.operators.size(), size_t{1});
  CHECK(is<AnyDistribution>(pipe.operators[0]->required_distribution()));
}

TEST("if with a stateful branch derives Single") {
  auto pipe = compile_to_ir("if x > 0 { summarize n=count() } else { drop z }");
  REQUIRE_EQUAL(pipe.operators.size(), size_t{1});
  CHECK(is<SingleDistribution>(pipe.operators[0]->required_distribution()));
}

TEST("pipeline requirement folds to the strongest") {
  auto pipe = compile_to_ir("where x > 0 | summarize n=count() | set y = 1");
  CHECK(is<SingleDistribution>(pipe.required_distribution()));
}

} // namespace tenzir
