//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/execution/pipeline.hpp"

namespace vast::execution {

/// A plan is a pipeline whose input and output types are void. Unlike a
/// pipeline, a plan is considered complete and executable, but cannot be used
/// as a logical operator itself.
class plan final {
public:
  /// Create a plan from a pipeline.
  /// @pre The pipeline must have the input and output types void.
  static auto make(std::unique_ptr<pipeline> pipeline) -> caf::expected<plan> {
    if (pipeline->input_type().id != detail::element_type_traits<void>::id) {
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("plan must have input type '{}'; found '{}'",
                    detail::element_type_traits<void>::name,
                    pipeline->input_type().name));
    }
    if (pipeline->output_type().id != detail::element_type_traits<void>::id) {
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("plan must have output type '{}'; found '{}'",
                    detail::element_type_traits<void>::name,
                    pipeline->output_type().name));
    }
    return plan{std::move(pipeline)};
  }

  /// Create a plan from an ordered sequence of operators.
  /// @pre The operator sequence must form a valid @ref pipeline.
  /// @pre The pipeline must have the input and output types void.
  static auto make(std::vector<logical_operator_ptr> logical_operators)
    -> caf::expected<plan> {
    auto pipeline = pipeline::make(std::move(logical_operators));
    if (!pipeline)
      return std::move(pipeline.error());
    return make(std::move(*pipeline));
  }

private:
  /// Construct a plan. This private constructor assumes that the pipeline has
  /// the input and output types void.
  explicit plan(std::unique_ptr<pipeline> pipeline) noexcept
    : pipeline_{std::move(pipeline)} {
    // nop
  }

  /// The plan's underlying pipeline.
  std::unique_ptr<pipeline> pipeline_ = {};
};

} // namespace vast::execution
