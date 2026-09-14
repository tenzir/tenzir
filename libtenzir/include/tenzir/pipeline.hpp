//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/tag.hpp"

#include <string_view>

namespace tenzir {

/// Variant of all types that can be used for operators.
///
/// @note During instantiation, a type `T` normally corresponds to
/// `generator<T>`. However, an input type of `void` corresponds to
/// sources (which receive a `std::monostate`) and an otuput type of `void`
/// corresponds to sinks (which return a `generator<std::monostate>`).
using operator_type = tag_variant<void, table_slice, chunk_ptr>;

/// Concept for pipeline operator input element types.
template <class T>
concept operator_input_batch
  = std::is_same_v<T, table_slice> or std::is_same_v<T, chunk_ptr>;

/// User-friendly name for the given pipeline batch type.
template <class T>
constexpr auto operator_type_name() -> std::string_view {
  if constexpr (std::is_same_v<T, void> or std::is_same_v<T, std::monostate>) {
    return "void";
  } else if constexpr (std::is_same_v<T, table_slice>) {
    return "events";
  } else if constexpr (std::is_same_v<T, chunk_ptr>) {
    return "bytes";
  } else {
    static_assert(detail::always_false_v<T>, "not a valid element type");
  }
}

/// @see `operator_type_name<T>()`.
inline auto operator_type_name(operator_type type) -> std::string_view {
  return std::visit(
    []<class T>(tag<T>) {
      return operator_type_name<T>();
    },
    type);
}

/// Returns a trivially-true expression. This is a workaround for having no
/// empty conjunction (yet). It can also be used in a comparison to detect that
/// an expression is trivially-true.
inline auto trivially_true_expression() -> const expression& {
  static auto expr = expression{
    predicate{
      meta_extractor{meta_extractor::kind::schema},
      relational_operator::not_equal,
      data{std::string{"this expression matches everything"}},
    },
  };
  return expr;
}

/// The operator location.
enum class operator_location {
  local,    ///< Run this operator in a local process, e.g., `tenzir exec`.
  remote,   ///< Run this operator at a node.
  anywhere, ///< Run this operator where the previous operator ran.
};

auto inspect(auto& f, operator_location& x) {
  return detail::inspect_enum_str(f, x, {"local", "remote", "anywhere"});
}

using serializer
  = std::variant<std::reference_wrapper<caf::serializer>,
                 std::reference_wrapper<caf::binary_serializer>,
                 std::reference_wrapper<caf::detail::stringification_inspector>>;

using deserializer
  = std::variant<std::reference_wrapper<caf::deserializer>,
                 std::reference_wrapper<caf::binary_deserializer>>;

/// See `operator_base::optimize` for a description of this.
enum class EventOrder {
  ordered,
  schema,
  unordered,
};

/// Returns the event order that allows for more optimization than the other.
inline auto weaker_event_order(EventOrder a, EventOrder b) -> EventOrder {
  return std::max(a, b);
}

/// Returns the event order that imposes the stricter requirement.
inline auto stronger_event_order(EventOrder a, EventOrder b) -> EventOrder {
  return std::min(a, b);
}

auto inspect(auto& f, EventOrder& x) -> bool {
  return detail::inspect_enum_str(f, x, {"ordered", "schema", "unordered"});
}

struct operator_measurement {
  std::string unit = std::string{operator_type_name<void>()};
  uint64_t num_elements = {};
  uint64_t num_batches = {};

  // Approximate byte amount for events, exact byte amount for bytes.
  uint64_t num_approx_bytes = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_measurement& x) -> bool {
    return f.object(x).pretty_name("metric").fields(
      f.field("unit", x.unit), f.field("num_elements", x.num_elements),
      f.field("num_batches", x.num_batches),
      f.field("num_approx_bytes", x.num_approx_bytes));
  }
};

// Metrics that track the information about inbound and outbound elements that
// pass through this operator.
struct [[nodiscard]] operator_metric {
  uint64_t operator_index = {};
  std::string operator_name = {};
  operator_measurement inbound_measurement = {};
  operator_measurement outbound_measurement = {};
  duration time_starting = {};
  duration time_to_first_input = {};
  duration time_processing = {};
  duration time_scheduled = {};
  duration time_total = {};
  duration time_running = {};
  duration time_paused = {};
  uint64_t num_runs = {};
  uint64_t num_runs_processing = {};
  uint64_t num_runs_processing_input = {};
  uint64_t num_runs_processing_output = {};

  // Whether this metric is considered internal or not; only external metrics
  // may be counted for ingress and egress.
  bool internal = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_metric& x) -> bool {
    return f.object(x).pretty_name("metric").fields(
      f.field("operator_index", x.operator_index),
      f.field("operator_name", x.operator_name),
      f.field("time_starting", x.time_starting),
      f.field("time_to_first_input", x.time_to_first_input),
      f.field("time_processing", x.time_processing),
      f.field("time_scheduled", x.time_scheduled),
      f.field("time_total", x.time_total),
      f.field("time_running", x.time_running),
      f.field("time_paused", x.time_paused),
      f.field("inbound_measurement", x.inbound_measurement),
      f.field("outbound_measurement", x.outbound_measurement),
      f.field("num_runs", x.num_runs),
      f.field("num_runs_processing", x.num_runs_processing),
      f.field("num_runs_processing_input", x.num_runs_processing_input),
      f.field("num_runs_processing_output", x.num_runs_processing_output),
      f.field("internal", x.internal));
  }

  static auto to_type() -> type {
    return {
      "tenzir.metrics.operator",
      record_type{
        {"pipeline_id", string_type{}},
        {"run", uint64_type{}},
        {"hidden", bool_type{}},
        {"operator_id", uint64_type{}},
        {"source", bool_type{}},
        {"transformation", bool_type{}},
        {"sink", bool_type{}},
        {"internal", bool_type{}},
        {"timestamp", time_type{}},
        {"duration", duration_type{}},
        {"starting_duration", duration_type{}},
        {"processing_duration", duration_type{}},
        {"scheduled_duration", duration_type{}},
        {"running_duration", duration_type{}},
        {"paused_duration", duration_type{}},
        {"input",
         record_type{
           {"unit", string_type{}},
           {"elements", uint64_type{}},
           {"approx_bytes", uint64_type{}},
           {"batches", uint64_type{}},
         }},
        {"output",
         record_type{
           {"unit", string_type{}},
           {"elements", uint64_type{}},
           {"approx_bytes", uint64_type{}},
           {"batches", uint64_type{}},
         }},
      },
      {{"internal", ""}},
    };
  }
};

} // namespace tenzir
