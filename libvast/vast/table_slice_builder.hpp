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

#include "vast/fwd.hpp"  // for table_slice_ptr
#include "vast/type.hpp" // for record_type
#include "vast/view.hpp" // for data_view

#include <caf/ref_counted.hpp> // for ref_counted

#include <stddef.h>    // for size_t
#include <type_traits> // for decay_t, is_same_v

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
  template <class T>
  [[nodiscard]] bool add(const T& x) {
    if constexpr (std::is_same_v<std::decay_t<T>, data_view>) {
      return add_impl(x);
    } else {
      return add_impl(make_view(x));
    }
  }

  /// Adds data to the builder.
  /// @param xs The data to add.
  /// @returns `true` on success.
  template <class T0, class T1, class... Ts>
  [[nodiscard]] bool add(const T0& x0, const T1& x1, const Ts&... xs) {
    return add(x0) && add(x1) && (add(xs) && ...);
  }

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

protected:
  // -- utilities -------------------------------------------------------------

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  virtual bool add_impl(data_view x) = 0;

private:
  record_type layout_;
};

/// @relates table_slice_builder
using table_slice_builder_ptr = caf::intrusive_ptr<table_slice_builder>;

/// @relates table_slice_builder
void intrusive_ptr_add_ref(const table_slice_builder* ptr);

/// @relates table_slice_builder
void intrusive_ptr_release(const table_slice_builder* ptr);

} // namespace vast
