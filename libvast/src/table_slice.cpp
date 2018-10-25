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

#include "vast/table_slice.hpp"

#include <caf/actor_system.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/execution_unit.hpp>
#include <caf/sec.hpp>
#include <caf/serializer.hpp>
#include <caf/sum_type.hpp>

#include "vast/const_table_slice_handle.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_handle.hpp"
#include "vast/value.hpp"

namespace vast {

namespace {

using size_type = table_slice::size_type;

auto cap (size_type pos, size_type num, size_type last) {
  return num == table_slice::npos ? last : std::min(last, pos + num);
}

} // namespace <anonymous>

table_slice::table_slice(record_type layout)
  : offset_(0),
    layout_(std::move(layout)),
    rows_(0),
    columns_(flat_size(layout_)) {
  // nop
}

table_slice::~table_slice() {
  // no
}

record_type table_slice::layout(size_type first_column,
                                size_type num_columns) const {
  if (first_column >= columns_)
    return {};
  auto col_begin = first_column;
  auto col_end = cap(first_column, num_columns, columns_);
  std::vector<record_field> sub_records{layout_.fields.begin() + col_begin,
                                        layout_.fields.begin() + col_end};
  return record_type{std::move(sub_records)};
}

caf::error table_slice::serialize_ptr(caf::serializer& sink,
                                      const_table_slice_ptr ptr) {
  if (!ptr) {
    record_type dummy;
    return sink(dummy);
  }
  return caf::error::eval([&] { return sink(ptr->layout()); },
                          [&] { return sink(ptr->implementation_id()); },
                          [&] { return ptr->serialize(sink); });
}

caf::error table_slice::deserialize_ptr(caf::deserializer& source,
                                        table_slice_ptr& ptr) {
  if (source.context() == nullptr)
    return caf::sec::no_context;
  record_type layout;
  caf::atom_value impl_id;
  auto err = caf::error::eval([&] { return source(layout); },
                              [&] { return source(impl_id); });
  if (err)
    return err;
  // Only default-constructed table slice handles have an empty layout.
  if (layout.fields.empty()) {
    ptr.reset();
    return caf::none;
  }
  ptr = make_table_slice(std::move(layout), source.context()->system(),
                         impl_id);
  if (!ptr)
    return ec::invalid_table_slice_type;
  return ptr->deserialize(source);
}

table_slice_ptr make_table_slice(record_type layout, caf::actor_system& sys,
                                 caf::atom_value impl) {
  if (impl == caf::atom("DEFAULT")) {
    auto ptr = caf::make_counted<default_table_slice>(std::move(layout));
    return ptr;
  }
  using generic_fun = caf::runtime_settings_map::generic_function_pointer;
  using factory_fun = table_slice_ptr (*)(record_type);
  auto val = sys.runtime_settings().get(impl);
  if (!caf::holds_alternative<generic_fun>(val)) {
    VAST_ERROR_ANON("table_slice",
                    "has no factory function for implementation key",
                    impl);
    return nullptr;
  }
  auto fun = reinterpret_cast<factory_fun>(caf::get<generic_fun>(val));
  return fun(std::move(layout));
}

void intrusive_ptr_add_ref(const table_slice* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const table_slice* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

bool operator==(const table_slice& x, const table_slice& y) {
  if (&x == &y)
    return true;
  if (x.rows() != y.rows()
      || x.columns() != y.columns()
      || x.layout() != y.layout())
    return false;
  for (size_t row = 0; row < x.rows(); ++row)
    for (size_t col = 0; col < x.columns(); ++col)
      if (x.at(row, col) != y.at(row, col))
        return false;
  return true;
}

} // namespace vast
