//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/variant_traits.hpp"

#include <chrono>
#include <string>
#include <vector>

using namespace tenzir;

namespace {

/// A context whose pipeline checkpoints.
class CheckpointingCtx final : public test::NovaOpCtx {
public:
  using NovaOpCtx::NovaOpCtx;

  auto checkpoint_settings() const
    -> Option<CheckpointSettings const&> override {
    return settings_;
  }

private:
  CheckpointSettings settings_{std::chrono::seconds{1}, false};
};

} // namespace

TEST("local aggregation requires a pipeline without checkpoints") {
  auto aggregation = ir::OptimizeAggregation{
    .columns = {},
    .schema_name = "tenzir.summarize",
    .fallback = {},
  };
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto const& reg = provider.as_session().reg();
  // Without checkpoints, a source may run the fallback in a subpipeline.
  auto plain = test::NovaOpCtx{dh, reg};
  CHECK(pushdown::can_aggregate_locally(aggregation, plain));
  CHECK(std::move(dh).collect().empty());
  // With checkpoints, the source fails like the `summarize` that it replaced,
  // instead of spawning a subpipeline that the executor cannot checkpoint.
  dh = collecting_diagnostic_handler{};
  auto checkpointing = CheckpointingCtx{dh, reg};
  CHECK(not pushdown::can_aggregate_locally(aggregation, checkpointing));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().severity, severity::error);
  CHECK_EQUAL(diagnostics.front().message,
              "`summarize` does not support checkpointing with `--nova` yet");
}

TEST("count, min, and max are the supported functions") {
  using Column = ir::OptimizeAggregation::Column;
  using Path = std::vector<std::string>;
  auto supports = [](std::vector<Column> columns) {
    return pushdown::supports_count_min_max(ir::OptimizeAggregation{
      .columns = std::move(columns),
      .schema_name = "tenzir.summarize",
      .fallback = {},
    });
  };
  auto key = Column{.output = {"k"}, .function = None{}, .input = Path{"k"}};
  auto count = Column{.output = {"n"}, .function = "count", .input = None{}};
  auto count_x
    = Column{.output = {"c"}, .function = "count", .input = Path{"x"}};
  auto min = Column{.output = {"lo"}, .function = "min", .input = Path{"x"}};
  auto max = Column{.output = {"hi"}, .function = "max", .input = Path{"x"}};
  CHECK(supports({}));
  CHECK(supports({key, count, count_x, min, max}));
  // `min` and `max` need an input, and other functions stay local.
  CHECK(not supports(
    {Column{.output = {"lo"}, .function = "min", .input = None{}}}));
  CHECK(not supports(
    {Column{.output = {"hi"}, .function = "max", .input = None{}}}));
  CHECK(not supports(
    {key, Column{.output = {"s"}, .function = "sum", .input = Path{"x"}}}));
}

TEST("classification names the result columns") {
  using Column = ir::OptimizeAggregation::Column;
  using Path = std::vector<std::string>;
  auto classify = [](std::vector<Column> columns) {
    return pushdown::classify(ir::OptimizeAggregation{
      .columns = std::move(columns),
      .schema_name = "tenzir.summarize",
      .fallback = {},
    });
  };
  auto plan = classify({
    Column{.output = {"k"}, .function = None{}, .input = Path{"a", "b"}},
    Column{.output = {"n"}, .function = "count", .input = None{}},
    Column{.output = {"c"}, .function = "count", .input = Path{"x"}},
    Column{.output = {"lo"}, .function = "min", .input = Path{"y"}},
    Column{.output = {"hi"}, .function = "max", .input = Path{"z"}},
  });
  REQUIRE(plan);
  REQUIRE_EQUAL(plan->columns.size(), size_t{5});
  for (auto i = size_t{0}; i < plan->columns.size(); ++i) {
    CHECK_EQUAL(plan->columns[i].alias, pushdown::aggregation_column(i));
  }
  auto const* key
    = try_as<pushdown::AggregationPlan::Key>(plan->columns[0].kind);
  REQUIRE(key);
  CHECK_EQUAL(key->input, (Path{"a", "b"}));
  auto const* count
    = try_as<pushdown::AggregationPlan::Count>(plan->columns[1].kind);
  REQUIRE(count);
  CHECK(not count->input);
  count = try_as<pushdown::AggregationPlan::Count>(plan->columns[2].kind);
  REQUIRE(count);
  CHECK_EQUAL(count->input, Option<Path>{Path{"x"}});
  auto const* min
    = try_as<pushdown::AggregationPlan::Min>(plan->columns[3].kind);
  REQUIRE(min);
  CHECK_EQUAL(min->input, Path{"y"});
  auto const* max
    = try_as<pushdown::AggregationPlan::Max>(plan->columns[4].kind);
  REQUIRE(max);
  CHECK_EQUAL(max->input, Path{"z"});
  // An empty aggregation has an empty plan.
  plan = classify({});
  REQUIRE(plan);
  CHECK(plan->columns.empty());
  // Other functions and group keys without an input have no plan.
  CHECK(not classify(
    {Column{.output = {"s"}, .function = "sum", .input = Path{"x"}}}));
  CHECK(not classify(
    {Column{.output = {"k"}, .function = None{}, .input = None{}}}));
}

TEST("classification may count the events of each group") {
  using Column = ir::OptimizeAggregation::Column;
  using Path = std::vector<std::string>;
  auto plan = pushdown::classify(
    ir::OptimizeAggregation{
      .columns
      = {Column{.output = {"k"}, .function = None{}, .input = Path{"k"}}},
      .schema_name = "tenzir.summarize",
      .fallback = {},
    },
    {.count_events = true});
  REQUIRE(plan);
  REQUIRE_EQUAL(plan->columns.size(), size_t{2});
  CHECK_EQUAL(plan->columns[1].alias, pushdown::aggregation_column(1));
  auto const* count
    = try_as<pushdown::AggregationPlan::Count>(plan->columns[1].kind);
  REQUIRE(count);
  CHECK(not count->input);
}
