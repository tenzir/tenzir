//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/operator/optimization.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

namespace {

template <class T>
concept HasOrder = requires(T x) { x.order; };
template <class T>
concept HasFilter = requires(T x) { x.filter; };
template <class T>
concept HasLimit = requires(T x) { x.limit; };
template <class T>
concept HasProjection = requires(T x) { x.projection; };

template <class Bundle>
struct Args {
  uint64_t limit = 42;
  Bundle optimization;
};

template <class Bundle>
concept Bindable = requires(Describer<Args<Bundle>> d) {
  d.optimization(&Args<Bundle>::optimization);
};

using OrderOnly = OptimizationArgs<opt::Order>;
using FilterOnly = OptimizationArgs<opt::Filter>;
using FilterLimit = OptimizationArgs<opt::Filter, opt::Limit>;
using ProjectionOnly = OptimizationArgs<opt::Projection>;
using ReadOptimization
  = OptimizationArgs<opt::Filter, opt::Limit, opt::Projection>;
using AllInputs
  = OptimizationArgs<opt::Order, opt::Filter, opt::Limit, opt::Projection>;

static_assert(HasOrder<OrderOnly> and not HasFilter<OrderOnly>
              and not HasLimit<OrderOnly> and not HasProjection<OrderOnly>);
static_assert(HasFilter<FilterOnly> and not HasOrder<FilterOnly>
              and not HasLimit<FilterOnly> and not HasProjection<FilterOnly>);
static_assert(HasFilter<FilterLimit> and HasLimit<FilterLimit>
              and not HasOrder<FilterLimit> and not HasProjection<FilterLimit>);
static_assert(HasProjection<ProjectionOnly> and not HasOrder<ProjectionOnly>
              and not HasFilter<ProjectionOnly>
              and not HasLimit<ProjectionOnly>);
static_assert(HasOrder<AllInputs> and HasFilter<AllInputs>
              and HasLimit<AllInputs> and HasProjection<AllInputs>);
static_assert(Bindable<AllInputs>);
static_assert(Bindable<OrderOnly> and Bindable<FilterOnly>
              and Bindable<FilterLimit> and Bindable<ProjectionOnly>
              and Bindable<ReadOptimization>);
static_assert(not Bindable<OptimizationArgs<opt::Limit>>);
static_assert(not Bindable<OptimizationArgs<opt::Order, opt::Limit>>);
static_assert(not Bindable<OptimizationArgs<opt::Limit, opt::Projection>>);

// The same runtime inputs support both a barrier and filter propagation.
auto describe_order(bool propagate) -> Description {
  auto d = Describer<Args<OrderOnly>>{};
  d.optimization(&Args<OrderOnly>::optimization);
  if (propagate) {
    return d.invariant_order_filter();
  }
  return d.without_optimize();
}

} // namespace

TEST("optimization bundle materializes selected inputs") {
  auto d = Describer<Args<ReadOptimization>>{};
  d.optimization(&Args<ReadOptimization>::optimization);
  auto desc = d.without_optimize();
  REQUIRE(desc.set_filter);
  REQUIRE(desc.set_limit);
  REQUIRE(desc.set_projection);
  CHECK(not desc.set_order);
  auto args = desc.make_args();
  auto& value = args.as<Args<ReadOptimization>>();
  CHECK(value.optimization.filter.empty());
  CHECK(not value.optimization.limit);
  CHECK(not value.optimization.projection);
  auto filter = ir::OptimizeFilter{
    ast::constant{true, location::unknown},
    ast::constant{false, location::unknown},
  };
  (*desc.set_filter)(args, filter);
  (*desc.set_limit)(args, uint64_t{7});
  (*desc.set_projection)(args, ir::OptimizeProjection{});
  CHECK_EQUAL(value.limit, uint64_t{42});
  CHECK_EQUAL(value.optimization.limit, uint64_t{7});
  REQUIRE_EQUAL(value.optimization.filter.size(), size_t{2});
  CHECK(is_true_literal(value.optimization.filter[0]));
  CHECK(not is_true_literal(value.optimization.filter[1]));
  REQUIRE(value.optimization.projection);
  CHECK(value.optimization.projection->empty());
  (*desc.set_projection)(args, None{});
  CHECK(not value.optimization.projection);
}

TEST("order binding does not select a rewrite policy") {
  for (auto propagate : {false, true}) {
    auto desc = describe_order(propagate);
    REQUIRE(desc.set_order);
    CHECK(not desc.set_filter);
    CHECK(not desc.set_limit);
    CHECK(not desc.set_projection);
    auto args = desc.make_args();
    auto& value = args.as<Args<OrderOnly>>();
    CHECK_EQUAL(value.optimization.order, EventOrder::ordered);
    (*desc.set_order)(args, EventOrder::unordered);
    CHECK_EQUAL(value.optimization.order, EventOrder::unordered);
    CHECK_EQUAL(value.limit, uint64_t{42});
    auto dh = collecting_diagnostic_handler{};
    auto ctx = DescribeCtx{{}, {}, {}, desc, location::unknown, dh};
    auto result = (*desc.optimizer)(
      ctx, ir::OptimizeRequest{
             .filter = {ast::constant{true, location::unknown}},
             .order = EventOrder::unordered,
             .limit = uint64_t{5},
             .projection = ir::OptimizeProjection{},
           });
    CHECK_EQUAL(result.order,
                propagate ? EventOrder::unordered : EventOrder::ordered);
    CHECK_EQUAL(result.filter_upstream.size(), propagate ? 1u : 0u);
    CHECK_EQUAL(result.filter_self.size(), propagate ? 0u : 1u);
    CHECK(not result.limit_upstream);
    CHECK(not result.projection_upstream);
  }
}

TEST("partial optimization bundles install only their setters") {
  auto filter = Describer<Args<FilterOnly>>{};
  filter.optimization(&Args<FilterOnly>::optimization);
  auto filter_desc = filter.without_optimize();
  CHECK(filter_desc.set_filter);
  CHECK(not filter_desc.set_order);
  CHECK(not filter_desc.set_limit);
  CHECK(not filter_desc.set_projection);
  auto filtered_limit = Describer<Args<FilterLimit>>{};
  filtered_limit.optimization(&Args<FilterLimit>::optimization);
  auto limit_desc = filtered_limit.without_optimize();
  CHECK(limit_desc.set_filter);
  CHECK(limit_desc.set_limit);
  CHECK(not limit_desc.set_order);
  CHECK(not limit_desc.set_projection);
  auto projection = Describer<Args<ProjectionOnly>>{};
  projection.optimization(&Args<ProjectionOnly>::optimization);
  auto projection_desc = projection.without_optimize();
  CHECK(projection_desc.set_projection);
  CHECK(not projection_desc.set_filter);
  CHECK(not projection_desc.set_order);
  CHECK(not projection_desc.set_limit);
  auto args = projection_desc.make_args();
  (*projection_desc.set_projection)(args, ir::OptimizeProjection{});
  REQUIRE(args.as<Args<ProjectionOnly>>().optimization.projection);
  CHECK(args.as<Args<ProjectionOnly>>().optimization.projection->empty());
}
