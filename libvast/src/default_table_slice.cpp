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

#include "vast/default_table_slice.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/default_table_slice_builder.hpp"
#include "vast/value_index.hpp"

namespace vast {

default_table_slice* default_table_slice::copy() const {
  return new default_table_slice(*this);
}

caf::error default_table_slice::serialize(caf::serializer& sink) const {
  return sink(xs_);
}

caf::error default_table_slice::deserialize(caf::deserializer& source) {
  return source(xs_);
}

void default_table_slice::append_column_to_index(size_type col,
                                                 value_index& idx) const {
  for (size_type row = 0; row < rows(); ++row)
    idx.append(make_view(caf::get<vector>(xs_[row])[col]), offset() + row);
}

data_view default_table_slice::at(size_type row, size_type col) const {
  VAST_ASSERT(row < rows());
  VAST_ASSERT(row < xs_.size());
  VAST_ASSERT(col < columns());
  auto& x = caf::get<vector>(xs_[row]);
  VAST_ASSERT(col < x.size());
  return make_view(x[col]);
}

table_slice_ptr default_table_slice::make(table_slice_header header) {
  return table_slice_ptr{new default_table_slice{std::move(header)}, false};
}

table_slice_ptr default_table_slice::make(record_type layout,
                                          const std::vector<vector>& rows) {
  default_table_slice_builder builder{std::move(layout)};
  for (auto& row : rows)
    for (auto& item : row)
      builder.add(make_view(item));
  auto result = builder.finish();
  VAST_ASSERT(result != nullptr);
  return result;
}

caf::atom_value default_table_slice::implementation_id() const noexcept {
  return class_id;
}

default_table_slice::default_table_slice(table_slice_header header)
  : table_slice{std::move(header)} {
  // nop
}

} // namespace vast
