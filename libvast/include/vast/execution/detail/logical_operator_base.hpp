//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/execution/detail/element_type.hpp"
#include "vast/execution/detail/physical_operator.hpp"
#include "vast/type.hpp"

namespace vast::execution::detail {

/// The common interface of all logical operators, regardless of their input and
/// output types. Commonly used as @ref logical_operator_ptr to store logical
/// operators of any kind with unique ownership.
class logical_operator_base {
public:
  /// Destroy the logical operator.
  virtual ~logical_operator_base() noexcept = default;

  /// The type-erased mechanism to create a physical from the logical operator
  /// given an input schema.
  /// @note Users typically do not override this function directly, but rather
  /// `logical_operator_mixin<Input, Output>::make(...)`, which in turn
  /// overrides this depending on the specified input and output element typs.
  /// @param input_schema The input schema for this instantiation of the logical
  /// operator. This function is only called once per unique schema. For input
  /// types that do not require a schema the none type is passed in.
  [[nodiscard]] virtual auto make_any(type input_schema)
    -> caf::expected<any_physical_operator>
    = 0;

  /// Returns the input element type.
  /// @note Users typically do not override this function directly, as @ref
  /// logical_operator implements it automatically depending on its specified
  /// element type.
  [[nodiscard]] virtual auto input_type() const noexcept -> runtime_element_type
    = 0;

  /// Returns the output element type.
  /// @note Users typically do not override this function directly, as @ref
  /// logical_operator implements it automatically depending on its specified
  /// element type.
  [[nodiscard]] virtual auto output_type() const noexcept
    -> runtime_element_type
    = 0;

  /// Returns a textual representation of the operator.
  [[nodiscard]] virtual auto to_string() const noexcept -> std::string = 0;
};

/// A helper type that provides a `make()` virtual function for logical
/// operators whose input type does not require a schema.
/// @related logical_operator_base
template <element_type Input, element_type Output>
struct logical_operator_mixin : public logical_operator_base {
  [[nodiscard]] virtual auto make()
    -> caf::expected<physical_operator<Input, Output>>
    = 0;
};

/// A helper type that provides a `make(type input_schema)` virtual function for
/// logical operators whose input type require a schema.
/// @related logical_operator_base
template <element_type Input, element_type Output>
  requires(element_type_traits<Input>::requires_schema)
struct logical_operator_mixin<Input, Output> : public logical_operator_base {
  [[nodiscard]] virtual auto make(type input_schema)
    -> caf::expected<physical_operator<Input, Output>>
    = 0;
};

} // namespace vast::execution::detail
