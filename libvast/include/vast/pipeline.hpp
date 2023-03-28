//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/dynamic_operator.hpp"

namespace vast {

/// A pipeline is a sequence of pipeline operators.
// TODO: Check types?
class pipeline final : public dynamic_operator {
public:
  /// Constructs an empty pipeline.
  pipeline() = default;

  /// Constructs a pipeline from a sequence of operators. Flattens nested
  /// pipelines, for example `(a | b) | c` becomes `a | b | c`.
  explicit pipeline(std::vector<operator_ptr> operators);

  /// Parses a logical pipeline from its textual representation. It is *not*
  /// guaranteed that `parse(to_string())` is equivalent to `*this`.
  // TODO: Or is it, if it succeeds?
  static auto parse(std::string_view repr) -> caf::expected<pipeline>;

  /// Returns the sequence of operators that this pipeline was built from.
  auto unwrap() && -> std::vector<operator_ptr> {
    return std::move(operators_);
  }

  auto instantiate(operator_input input, operator_control_plane& control) const
    -> caf::expected<operator_output> override;

  auto copy() const -> operator_ptr override;

  auto to_string() const -> std::string override;

private:
  std::vector<operator_ptr> operators_;
};

/// Returns a generator that, when advanced, incrementally executes the given
/// pipeline on the current thread.
auto make_local_executor(pipeline p) -> generator<caf::expected<void>>;

} // namespace vast
