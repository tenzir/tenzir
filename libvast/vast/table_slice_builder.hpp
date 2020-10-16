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

// -- v1 includes --------------------------------------------------------------

#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_encoding.hpp"

#include <caf/make_counted.hpp>

#include <flatbuffers/flatbuffers.h>

// -- v0 includes --------------------------------------------------------------

#include "vast/fwd.hpp"
#include "vast/view.hpp"

#include <caf/make_counted.hpp>
#include <caf/ref_counted.hpp>

#include <type_traits>

namespace vast {

namespace v1 {

/// Enables incremental construction of a table slice.
/// @relates table_slice
class table_slice_builder : public caf::ref_counted {
public:
  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a table slice builder from a layout.
  explicit table_slice_builder(record_type layout);

  /// Destroys a table slice builder.
  virtual ~table_slice_builder();

  // -- factory facade ---------------------------------------------------------

  /// The default implementation builds invalid table slices.
  static constexpr inline auto implementation_id
    = table_slice_encoding::invalid;

  // -- properties -------------------------------------------------------------

  /// Calls `add(x)` as long as `x` is not a vector, otherwise calls `add(y)`
  /// for each `y` in `x`.
  [[nodiscard]] bool recursive_add(const data& x, const type& t);

  /// Adds data to the builder.
  /// @param xs The data to add.
  /// @returns `true` on success.
  template <class T, class... Ts>
  [[nodiscard]] bool add(const T& x, const Ts&... xs) {
    if constexpr (sizeof...(Ts) == 0) {
      if constexpr (std::is_same_v<std::decay_t<T>, data_view>)
        return add_impl(x);
      else
        return add_impl(make_view(x));
    } else {
      return add(x) && (add(xs) && ...);
    }
  }

  /// Constructs a table_slice from the currently accumulated state and resets
  /// the internal state.
  /// @returns A table slice from the accumulated calls to add, or an error.
  [[nodiscard]] table_slice finish();

  /// Reset the builder state.
  void reset();

  /// @returns the current number of rows in the table slice.
  virtual table_slice::size_type rows() const noexcept;

  /// @returns an identifier for the implementing class.
  virtual table_slice_encoding encoding() const noexcept;

  /// Allows the table slice builder to allocate sufficient storage.
  /// @param num_rows The number of rows to allocate storage for.
  virtual void reserve(table_slice::size_type num_rows);

  /// @returns The table layout.
  const record_type& layout() const noexcept;

  /// @returns The number of columns in the table slice.
  table_slice::size_type columns() const noexcept;

protected:
  // -- implementation details -------------------------------------------------

  /// Adds data to the builder.
  /// @param x The data to add.
  /// @returns `true` on success.
  virtual bool add_impl(data_view x);

  /// Constructs a table_slice from the currently accumulated state.
  /// @returns A chunk from the accumulated calls to add that matches the
  /// encoding of the builder implementation.
  virtual caf::expected<chunk_ptr> finish_impl();

  /// Reset the builder.
  virtual void reset_impl();

  /// The underlying builder for FlatBuffers.
  flatbuffers::FlatBufferBuilder fbb_;

private:
  /// The layout of the to-be-constructed table slices.
  record_type layout_;
};

// -- intrusive_ptr facade -----------------------------------------------------

/// @relates table_slice_builder
void intrusive_ptr_add_ref(const table_slice_builder* ptr);

/// @relates table_slice_builder
void intrusive_ptr_release(const table_slice_builder* ptr);

} // namespace v1

inline namespace v0 {

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
  [[nodiscard]] virtual table_slice_ptr finish() = 0;

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
void intrusive_ptr_add_ref(const table_slice_builder* ptr);

/// @relates table_slice_builder
void intrusive_ptr_release(const table_slice_builder* ptr);

} // namespace v0

} // namespace vast
