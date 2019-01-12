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

#include "vast/table_slice_builder.hpp"

#include <algorithm>

#include "vast/data.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/detail/overload.hpp"

namespace vast {

namespace {

std::unordered_map<caf::atom_value, table_slice_builder_factory> factories_;

} // namespace <anonymous>

table_slice_builder::table_slice_builder(record_type layout)
  : layout_(std::move(layout)) {
  // nop
}

table_slice_builder::~table_slice_builder() {
  // nop
}

bool table_slice_builder::recursive_add(const data& x, const type& t) {
  return caf::visit(detail::overload(
                      [&](const vector& xs, const record_type& rt) {
                        for (size_t i = 0; i < xs.size(); ++i) {
                          if (!recursive_add(xs[i], rt.fields[i].type))
                            return false;
                        }
                        return true;
                      },
                      [&](const auto&, const auto&) {
                        return add(make_view(x));
                      }),
                    x, t);
}

void table_slice_builder::reserve(size_t) {
  // nop
}

size_t table_slice_builder::columns() const noexcept {
  return layout_.fields.size();
}

bool add_table_slice_builder_factory(caf::atom_value id,
                                     table_slice_builder_factory f) {
  if (factories_.count(id) > 0)
    return false;
  factories_.emplace(id, f);
  return true;
}

table_slice_builder_factory
get_table_slice_builder_factory(caf::atom_value id) {
  if (id == caf::atom("default"))
    return default_table_slice_builder::make;
  auto i = factories_.find(id);
  return i != factories_.end() ? i->second : nullptr;
}

table_slice_builder_ptr make_table_slice_builder(caf::atom_value id,
                                                 record_type layout) {
  if (auto f = get_table_slice_builder_factory(id))
    return (*f)(std::move(layout));
  return nullptr;
}

} // namespace vast
