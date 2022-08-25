//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/table_slice.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/ref_counted.hpp>

#include <span>
#include <type_traits>

namespace vast {

/// Enables incremental construction of a table slice.
/// @relates table_slice
class table_slice_builder : public caf::ref_counted {
public:
  // -- types and constants ----------------------------------------------------

  /// The default size of the buffer that the builder works with.
  static constexpr size_t default_buffer_size = 8192;

  // -- constructors, destructors, and assignment operators --------------------

  /// Forbid default-construction.
  table_slice_builder() = delete;

  /// Forbid copy-construction.
  table_slice_builder(const table_slice_builder&) = delete;

  /// Forbid copy-assignment.
  table_slice_builder& operator=(const table_slice_builder&) = delete;

  /// Forbid move-construction.
  table_slice_builder(table_slice_builder&&) = delete;

  /// Forbid move-assignment.
  table_slice_builder& operator=(table_slice_builder&&) = delete;

  /// Destroys a table slice builder.
  virtual ~table_slice_builder() noexcept;

  // -- properties -------------------------------------------------------------

  /// Calls `add(x)` as long as `x` is not a vector, otherwise calls `add(y)`
  /// for each `y` in `x`.
  [[nodiscard]] bool recursive_add(const data& x, const type& t);

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @param xs... The data to add.
  /// @returns `true` on success.
  template <class T, class... Ts>
  [[nodiscard]] bool add(const T& x, const Ts&... xs) {
    if constexpr (sizeof...(xs) == 0) {
      if constexpr (std::is_same_v<T, data_view>) {
        return add_impl(x);
      } else if constexpr (caf::detail::tl_contains<data_view::types, T>::value) {
        return add_impl(x);
      } else {
        return add_impl(make_view(x));
      }
    } else {
      return add(x) && (add(xs) && ...);
    }
  }

  /// Constructs a table_slice from the currently accumulated state. After
  /// calling this function, implementations must reset their internal state
  /// such that subsequent calls to add will restart with a new table_slice.
  /// @returns A table slice from the accumulated calls to add.
  /// @note Returns an invalid table slice on failure.
  [[nodiscard]] virtual table_slice finish() = 0;

  /// @returns The current number of rows in the table slice.
  virtual size_t rows() const noexcept = 0;

  /// @returns The number of columns in the table slice.
  size_t columns() const noexcept;

  /// @returns The table layout.
  const type& layout() const noexcept;

  /// @returns An identifier for the implementing class.
  virtual table_slice_encoding implementation_id() const noexcept = 0;

  /// Allows The table slice builder to allocate sufficient storage.
  /// @param `num_rows` The number of rows to allocate storage for.
  /// @note The default implementation does nothing.
  virtual void reserve(size_t num_rows);

  /// Opt-in to CAF's type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice_builder& x) {
    return f.object(x)
      .pretty_name("vast.table_slice_builder")
      .fields(f.field("layout", x.layout_));
  }

protected:
  // -- implementation utilities -----------------------------------------------

  /// Construct a builder for tables slices.
  /// @param layout The table layout.
  explicit table_slice_builder(type layout) noexcept;

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  virtual bool add_impl(data_view x) = 0;

private:
  // -- implementation details -------------------------------------------------

  type layout_;
};

// -- intrusive_ptr facade -----------------------------------------------------

/// @relates table_slice_builder
void intrusive_ptr_add_ref(const table_slice_builder* ptr);

/// @relates table_slice_builder
void intrusive_ptr_release(const table_slice_builder* ptr);

} // namespace vast
