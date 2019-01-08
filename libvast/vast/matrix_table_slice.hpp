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
#include <caf/make_copy_on_write.hpp>

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

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a matrix table slice with all elements.
  static table_slice_ptr make(table_slice_header header) {
    return table_slice_ptr{new matrix_table_slice{std::move(header)}, false};
  }

  /// Constructs a matrix table slice with all elements.
  static table_slice_ptr make(record_type layout, std::vector<data> xs) {
    table_slice_header header;
    header.layout = std::move(layout);
    auto ptr = new matrix_table_slice{std::move(header)};
    ptr->header_.rows = xs.size() / ptr->columns();
    ptr->storage_ = std::move(xs);
    return table_slice_ptr{ptr, false};
  }

  // -- properties -------------------------------------------------------------

  matrix_table_slice* copy() const override {
    return new matrix_table_slice{*this};
  }

  caf::error serialize(caf::serializer& sink) const override {
    return sink(storage_);
  }

  caf::error deserialize(caf::deserializer& source) override {
    return source(storage_);
  }

  caf::atom_value implementation_id() const noexcept override {
    return class_id;
  }

  data_view at(size_type row, size_type col) const override {
    auto i = LayoutPolicy::index_of(this->rows(), this->columns(), row, col);
    return make_view(storage_[i]);
  }

  void append_column_to_index(size_type col, value_index& idx) const override {
    auto row = offset();
    for (auto& x : column(col))
      idx.append(make_view(x), row++);
  }

private:
  // -- constructors, destructors, and assignment operators --------------------

  matrix_table_slice(table_slice_header header) : super{std::move(header)} {
    // nop
  }

  auto column(size_type pos) const {
    VAST_ASSERT(!storage_.empty());
    auto ptr = storage_.data();
    auto first = LayoutPolicy::make_column_iterator(ptr, this->rows(),
                                                    this->columns(), pos);
    using iterator_type = decltype(first);
    return detail::iterator_range<iterator_type>{first, first + this->rows()};
  }

  std::vector<data> storage_;
};

/// A matrix table slice with row-major memory order.
using row_major_matrix_table_slice
  = matrix_table_slice<policy::row_major<data>>;

/// A matrix table slice with column-major memory order.
using column_major_matrix_table_slice
  = matrix_table_slice<policy::column_major<data>>;

} // namespace vast
