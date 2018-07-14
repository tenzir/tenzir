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
#include <caf/make_counted.hpp>
#include <caf/serializer.hpp>

#include "vast/default_table_slice_builder.hpp"

namespace vast {

default_table_slice::default_table_slice(record_type layout)
  : table_slice{std::move(layout)} {
  // nop
}

table_slice_ptr default_table_slice::clone() const {
  return caf::make_counted<default_table_slice>(*this);
}

caf::error default_table_slice::save(caf::serializer& sink) {
  return sink(offset_, xs_);
}

caf::error default_table_slice::load(caf::deserializer& source) {
  auto err = source(offset_, xs_);
  rows_ = xs_.size();
  return err;
}

caf::optional<data_view>
default_table_slice::at(size_type row, size_type col) const {
  VAST_ASSERT(row < rows_);
  VAST_ASSERT(row < xs_.size());
  VAST_ASSERT(col < columns_);
  if (auto x = caf::get_if<vector>(&xs_[row])) {
    VAST_ASSERT(col < x->size());
    return make_view((*x)[col]);
  }
  return {};
}

table_slice_builder_ptr default_table_slice::make_builder(record_type layout) {
  using namespace detail;
  return caf::make_counted<default_table_slice_builder>(std::move(layout));
}

table_slice_ptr default_table_slice::make(record_type layout,
                                          const std::vector<vector>& rows) {
  auto builder = make_builder(std::move(layout));
  for (auto& row : rows)
    for (auto& item : row)
      builder->add(make_view(item));
  auto result = builder->finish();
  VAST_ASSERT(result != nullptr);
  return result;
}

caf::atom_value default_table_slice::implementation_id() const noexcept {
  return caf::atom("DEFAULT");
}

} // namespace vast
