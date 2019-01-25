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

#include "vast/format/multi_layout_reader.hpp"

#include "vast/error.hpp"
#include "vast/factory.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

namespace vast::format {

multi_layout_reader::multi_layout_reader(caf::atom_value table_slice_type)
  : reader(table_slice_type) {
  // nop
}

multi_layout_reader::~multi_layout_reader() {
  // nop
}

caf::error multi_layout_reader::finish(consumer& f,
                                       table_slice_builder_ptr& builder_ptr,
                                       caf::error result) {
  if (builder_ptr != nullptr && builder_ptr->rows() > 0) {
    auto ptr = builder_ptr->finish();
    // Override error in case we encounter an error in the builder.
    if (ptr == nullptr)
      return make_error(ec::parse_error, "unable to finish current slice");
    f(std::move(ptr));
  }
  return result;
}

caf::error multi_layout_reader::finish(consumer& f, caf::error result) {
  for (auto& kvp : builders_)
    if (auto err = finish(f, kvp.second))
      return err;
  return result;
}

table_slice_builder_ptr multi_layout_reader::builder(const type& t) {
  auto i = builders_.find(t);
  if (i != builders_.end())
    return i->second;
  if (!caf::holds_alternative<record_type>(t)) {
    VAST_ERROR(this,
               "cannot create slice builder for non-record type:", VAST_ARG(t));
    // Insert a nullptr into the map and return it to make sure the error gets
    // printed only once.
    return builders_[t];
  }
  auto ptr = factory<table_slice_builder>::make(table_slice_type_,
                                                caf::get<record_type>(t));
  builders_.emplace(t, ptr);
  return ptr;
}


} // namespace vast::format
