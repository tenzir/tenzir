//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/expected.hpp>

namespace vast {

/// An aggregation function; used by the *summarize* pipeline operator to
/// aggregate elements of matching rows.
/// @related aggregation_function_plugin
class aggregation_function {
public:
  virtual ~aggregation_function() noexcept = default;

  /// Return the output type of the function.
  /// @post result must not be the null type.
  [[nodiscard]] virtual type output_type() const = 0;

  /// Add data to the aggregation function.
  /// @param view The value to add.
  /// @pre *view* is either *null* or matches the input type.
  virtual void add(const data_view& view) = 0;

  /// Bulk-add data to the aggregation function.
  /// @param array The array ot add.
  /// @pre *array* matches the input type.
  /// @note The default implementation for this calls *add* repeatedly for all
  /// elements of the *array*.
  virtual void add(const arrow::Array& array);

  /// Finish the aggregation into a single materialized value.
  [[nodiscard]] virtual caf::expected<data> finish() && = 0;

  /// Return the input type of the function.
  [[nodiscard]] const type& input_type() const noexcept;

protected:
  /// Constructs the aggregation function. Must be called from implementing base
  /// classes.
  /// @param input_type The input type from the aggregation function plugin.
  explicit aggregation_function(type input_type) noexcept;

private:
  /// The input type of the function.
  type input_type_ = {};
};

} // namespace vast
