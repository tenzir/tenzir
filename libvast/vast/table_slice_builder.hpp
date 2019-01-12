/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <caf/make_counted.hpp>
#include <caf/ref_counted.hpp>

#include "vast/fwd.hpp"
#include "vast/view.hpp"

namespace vast {

/// Enables incremental construction of a table slice.
/// @relates table_slice
class table_slice_builder : public caf::ref_counted {
public:
  // -- constructors, destructors, and assignment operators --------------------

  table_slice_builder(record_type layout);

  ~table_slice_builder();

  // -- properties -------------------------------------------------------------

  /// Calls `add(x)` as long as `x` is not a vector, otherwise calls `add(y)`
  /// for each `y` in `x`.
  bool recursive_add(const data& x, const type& t);

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  virtual bool add(data_view x) = 0;

  /// Constructs a table_slice from the currently accumulated state. After
  /// calling this function, implementations must reset their internal state
  /// such that subsequent calls to add will restart with a new table_slice.
  /// @returns A table slice from the accumulated calls to add or `nullptr` on
  ///          failure.
  virtual table_slice_ptr finish() = 0;

  /// @returns the current number of rows in the table slice.
  virtual size_t rows() const noexcept = 0;

  /// @returns an identifier for the implementing class.
  virtual caf::atom_value implementation_id() const noexcept = 0;

  /// Allows the table slice builder to allocate sufficient storage for up to
  /// `num_rows` rows.
  virtual void reserve(size_t num_rows);

  /// @returns the table layout.
  const record_type& layout() const noexcept {
    return layout_;
  }

  /// @returns the number of columns in the table slice.
  size_t columns() const noexcept;

private:
  record_type layout_;
};

/// @relates table_slice_builder
using table_slice_builder_ptr = caf::intrusive_ptr<table_slice_builder>;

/// The factory function to construct a table slice builder for a layout.
/// @relates table_slice_builder
using table_slice_builder_factory = table_slice_builder_ptr (*)(record_type);

/// Registers a table slice builder factory.
/// @param id The unique implementation ID for the table slice type.
/// @param f The factory how to construct the table slice builder.
/// @returns `true` iff the *f* was successfully associated with *id*.
/// @relates table_slice get_table_slice_builder_factory
bool add_table_slice_builder_factory(caf::atom_value id,
                                     table_slice_builder_factory f);

/// Convenience overload for the two-argument version of this function.
template <class T>
bool add_table_slice_builder_factory(caf::atom_value id) {
  static auto factory = [](record_type layout) {
    return T::make(std::move(layout));
  };
  return add_table_slice_builder_factory(id, factory);
}

/// Retrieves a table slice builder factory.
/// @relates table_slice_builder add_table_slice_builder_factory
table_slice_builder_factory get_table_slice_builder_factory(caf::atom_value id);

/// Constructs a builder for a given table slice type.
/// @param id The (registered) implementation ID of the slice type.
/// @returns A table slice builder pointer or `nullptr` on failure.
/// @relates table_slice_builder add_table_slice_builder_factory
table_slice_builder_ptr make_table_slice_builder(caf::atom_value id,
                                                 record_type layout);

} // namespace vast
