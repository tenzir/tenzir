//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"
#include "tenzir/pipeline.hpp"

#include <concepts>

namespace tenzir {

namespace opt {

struct Order {
  /// The weakest downstream ordering requirement seen across optimization passes.
  EventOrder order = EventOrder::ordered;
};

struct Filter {
  /// Accepted predicates, in execution order. The runtime must enforce these:
  /// their downstream `where` operators have been removed.
  ir::OptimizeFilter filter;
};

struct Limit {
  /// A bound on events after the accepted filter chain. Keep this immutable
  /// during execution and track the remaining count separately.
  Option<uint64_t> limit;
};

struct Projection {
  /// `None` requires all fields; an empty projection requires no output fields.
  /// Also retain dependencies of accepted filters and the operator's own work.
  /// This is a hint: the downstream `select` remains authoritative.
  Option<ir::OptimizeProjection> projection;
};

struct Aggregation {
  /// The aggregation that the operator emits instead of its events, or `None`
  /// if it emits its events. It applies after the accepted filter chain.
  ///
  /// The operator must emit exactly the result of the aggregation. If it
  /// cannot compute the result itself, for example because a filter stays
  /// local, it runs `aggregation->fallback` on the events it would otherwise
  /// emit. See `Describer::aggregation` for when the optimizer sets this.
  Option<ir::OptimizeAggregation> aggregation;
};

} // namespace opt

template <class T>
concept OptimizationInput
  = std::same_as<T, opt::Order> or std::same_as<T, opt::Filter>
    or std::same_as<T, opt::Limit> or std::same_as<T, opt::Projection>
    or std::same_as<T, opt::Aggregation>;

/// Selected runtime inputs, not user arguments or optimizer rewrite policy.
/// Inheritance provides ordinary named access only to selected fields.
/// Repeated passes append filters, minimize limits, intersect projection hints,
/// and weaken ordering. Binding this bundle does not enable propagation.
template <OptimizationInput... Inputs>
struct OptimizationArgs : Inputs... {
  template <class T>
  static constexpr auto contains = (std::same_as<T, Inputs> or ...);
};

} // namespace tenzir
