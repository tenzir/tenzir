//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/operator_control_plane.hpp"
#include "vast/physical_operator.hpp"

#include <string_view>

namespace vast {

/// A type-erased version of a logical operator, and the base class of all
/// logical operators. Commonly used as *logical_operator_ptr*.
class runtime_logical_operator {
public:
  /// Destroys the logical operator.
  virtual ~runtime_logical_operator() noexcept = default;

  /// Returns the logical operator's input element type.
  [[nodiscard]] virtual auto input_element_type() const noexcept
    -> runtime_element_type
    = 0;

  /// Returns the logical operator's output element type.
  [[nodiscard]] virtual auto output_element_type() const noexcept
    -> runtime_element_type
    = 0;

  /// Returns whether this logical operator prefers to be run on its own thread,
  /// if the executor supports it.
  [[nodiscard]] virtual auto detached() const noexcept -> bool {
    return false;
  }

  /// Creates a physical operator from this logical operator for a given input
  /// schema.
  ///
  /// Involved objects are destroyed in the following order during pipeline
  /// execution (first to last):
  /// - Producers (i.e., the generator coroutines created by passing the
  ///   previous producer to a physical operator)
  /// - Physical operators (created per schema from the logical operator).
  /// - The logical operator.
  /// - The operator control plane.
  [[nodiscard]] virtual auto
  make_runtime_physical_operator(const type& input_schema,
                                 operator_control_plane* ctrl) noexcept
    -> caf::expected<runtime_physical_operator>
    = 0;

  /// Returns `true` if all current and future instances are done in the sense
  /// that they require no more input and will become exhausted eventually. This
  /// is in particular useful for the `head` operator. Returning `false` here is
  /// always sound, but can be a pessimization.
  [[nodiscard]] virtual auto done() const noexcept -> bool {
    return false;
  }

  /// Returns the textual representation of this operator.
  [[nodiscard]] virtual auto to_string() const noexcept -> std::string = 0;
};

/// A short-hand form for a uniquely owned logical operator.
using logical_operator_ptr = std::unique_ptr<runtime_logical_operator>;

/// A logical operator with known input and output element types.
template <element_type Input, element_type Output>
class logical_operator : public runtime_logical_operator {
protected:
  /// See *runtime_logical_operator::make_runtime_physical_operator(input_schema,
  /// ctrl)*.
  [[nodiscard]] virtual auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane* ctrl) noexcept
    -> caf::expected<physical_operator<Input, Output>>
    = 0;

  /// See *runtime_logical_operator::input_element_type()*
  [[nodiscard]] auto input_element_type() const noexcept
    -> runtime_element_type final {
    return element_type_traits<Input>{};
  }

  /// See *runtime_logical_operator::output_element_type()*
  [[nodiscard]] auto output_element_type() const noexcept
    -> runtime_element_type final {
    return element_type_traits<Output>{};
  }

  /// See *runtime_logical_operator::make_runtime_physical_operator(input_schema,
  /// ctrl)*
  [[nodiscard]] auto
  make_runtime_physical_operator(const type& input_schema,
                                 operator_control_plane* ctrl) noexcept
    -> caf::expected<runtime_physical_operator> final {
    auto op = make_physical_operator(input_schema, ctrl);
    if (not op)
      return std::move(op.error());
    return runtime_physical_operator{std::move(*op)};
  }
};

} // namespace vast

template <>
struct fmt::formatter<vast::logical_operator_ptr>
  : fmt::formatter<std::string_view> {
  template <class FormatContext>
  auto
  format(const vast::logical_operator_ptr& value, FormatContext& ctx) const {
    return fmt::formatter<std::string_view>::format(value->to_string(), ctx);
  }
};
