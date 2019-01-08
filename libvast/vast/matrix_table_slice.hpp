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

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <new>
#include <type_traits>

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/data.hpp"
#include "vast/detail/range.hpp"
#include "vast/policy/column_major.hpp"
#include "vast/policy/row_major.hpp"
#include "vast/table_slice.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

namespace vast {

/// An implementation of `table_slice` that keeps all entries in a
/// two-dimensional matrix, allocated in a single chunk of memory. As a
/// consequence, this table slice cannot grow and users have to provide the
/// maximum size upfront.
template <class LayoutPolicy>
class matrix_table_slice final : public table_slice {
public:
  // -- constants --------------------------------------------------------------

  static constexpr auto class_id = LayoutPolicy::class_id;

  // -- member types -----------------------------------------------------------

  /// Base type.
  using super = table_slice;

  /// Unsigned integer type.
  using size_type = super::size_type;

  /// Iterator over stored elements in unspecified order.
  using iterator = data*;

  /// Iterator over stored elements in unspecified order.
  using const_iterator = const data*;

  /// Iterator over stored elements in column-first order.
  using column_iterator = typename LayoutPolicy::column_iterator;

  /// Iterator over stored elements in column-first order.
  using const_column_iterator = typename LayoutPolicy::const_column_iterator;

  // -- constructors, destructors, and assignment operators --------------------

  ~matrix_table_slice() {
    // The element blocks gets allocated alongside this object. Hence, we need
    // to destroy our elements manually.
    std::destroy(begin(), end());
  }

  /// @warning leaves all elements uninitialized.
  static matrix_table_slice* make_uninitialized(table_slice_header header);

  static table_slice_ptr make(table_slice_header header) {
    auto ptr = make_uninitialized(std::move(header));
    std::uninitialized_default_construct(ptr->begin(), ptr->end());
    return table_slice_ptr{ptr, false};
  }

  // -- properties -------------------------------------------------------------

  matrix_table_slice* copy() const override {
    auto ptr = make_uninitialized(this->header());
    std::uninitialized_copy(begin(), end(), ptr->begin());
    return ptr;
  }

  caf::error serialize(caf::serializer& sink) const override {
    for (auto i = begin(); i != end(); ++i)
      if (auto err = sink(*i))
        return err;
    return caf::none;
  }

  caf::error deserialize(caf::deserializer& source) override {
    for (auto i = begin(); i != end(); ++i)
      if (auto err = source(*i))
        return err;
    return caf::none;
  }

  void append_column_to_index(size_type col, value_index& idx) const override {
    auto row = offset();
    for (auto& x : column(col))
      idx.append(make_view(x), row++);
  }

  caf::atom_value implementation_id() const noexcept override {
    return class_id;
  }

  data_view at(size_type row, size_type col) const override {
    auto ptr = storage();
    return make_view(ptr[LayoutPolicy::index_of(rows(), columns(), row, col)]);
  }

  iterator begin() {
    return storage();
  }

  iterator end() {
    return begin() + rows() * columns();
  }

  const_iterator begin() const {
    return storage();
  }

  const_iterator end() const {
    return begin() + rows() * columns();
  }

  /// @returns the range representing the column at position `pos`.
  detail::iterator_range<column_iterator> column(size_type pos) {
    auto first = LayoutPolicy::make_column_iterator(storage(), rows(),
                                                    columns(), pos);
    return {first, first + rows()};
  }

  /// @returns the range representing the column at position `pos`.
  detail::iterator_range<const_column_iterator> column(size_type pos) const {
    auto first = LayoutPolicy::make_column_iterator(storage(), rows(),
                                                    columns(), pos);
    return {first, first + rows()};
  }

  /// @returns a pointer to the first element.
  data* storage();

  /// @returns a pointer to the first element.
  const data* storage() const {
    // We restore the const when returning from this member function.
    return const_cast<matrix_table_slice*>(this)->storage();
  }

  // -- memory management ------------------------------------------------------

  void request_deletion(bool) const noexcept override {
    // This object was allocated using `malloc`. Hence, we need to call `free`
    // instead of `delete` here.
    this->~matrix_table_slice();
    free(const_cast<matrix_table_slice*>(this));
  }

private:
  // -- constructors, destructors, and assignment operators --------------------

  explicit matrix_table_slice(table_slice_header header) : super{std::move(header)} {
    // nop
  }
};

// Needs to be out-of-line, because it needs to call sizeof(matrix_table_slice).
template <class LayoutPolicy>
matrix_table_slice<LayoutPolicy>*
matrix_table_slice<LayoutPolicy>::make_uninitialized(
  table_slice_header header) {
  using impl = matrix_table_slice;
  using storage = std::aligned_storage_t<sizeof(impl), alignof(impl)>;
  using element_storage = std::aligned_storage_t<sizeof(data), alignof(data)>;
  auto columns = header.layout.fields.size();
  auto vptr = malloc(sizeof(storage)
                     + sizeof(element_storage) * header.rows * columns);
  // Construct only the table slice object.
  auto ptr = new (vptr) impl{std::move(header)};
  return ptr;
}

// Needs to be out-of-line, because it needs to call sizeof(matrix_table_slice).
template <class LayoutPolicy>
data* matrix_table_slice<LayoutPolicy>::storage() {
  // We always allocate the first data directly following this object.
  using storage = std::aligned_storage_t<sizeof(matrix_table_slice),
                                         alignof(matrix_table_slice)>;
  auto first = reinterpret_cast<unsigned char*>(this) + sizeof(storage);
  return reinterpret_cast<data*>(first);
}

/// A matrix table slice with row-major memory order.
using row_major_matrix_table_slice
  = matrix_table_slice<policy::row_major<data>>;

/// A matrix table slice with column-major memory order.
using column_major_matrix_table_slice
  = matrix_table_slice<policy::column_major<data>>;

} // namespace vast
