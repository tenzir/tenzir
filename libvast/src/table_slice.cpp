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
#include <caf/actor_system_config.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/execution_unit.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/sec.hpp>
#include <caf/serializer.hpp>
#include <caf/sum_type.hpp>

#include "vast/default_table_slice.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/format/test.hpp"
#include "vast/logger.hpp"
#include "vast/value.hpp"
#include "vast/value_index.hpp"

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
                                      const table_slice_ptr& ptr) {
  if (!ptr) {
    record_type dummy;
    return sink(dummy);
  }
  return caf::error::eval([&] { return sink(ptr->layout()); },
                          [&] { return sink(ptr->implementation_id()); },
                          [&] { return sink(ptr->rows()); },
                          [&] { return ptr->serialize(sink); });
}

caf::error table_slice::deserialize_ptr(caf::deserializer& source,
                                        table_slice_ptr& ptr) {
  if (source.context() == nullptr)
    return caf::sec::no_context;
  record_type layout;
  if (auto err = source(layout))
    return err;
  // Only default-constructed table slice handles have an empty layout.
  if (layout.fields.empty()) {
    ptr.reset();
    return caf::none;
  }
  caf::atom_value impl_id;
  table_slice::size_type impl_rows;
  auto err = caf::error::eval([&] { return source(impl_id); },
                              [&] { return source(impl_rows); });
  if (err)
    return err;
  // Construct a table slice of proper type.
  ptr = make_table_slice(std::move(layout), source.context()->system(),
                         impl_id, impl_rows);
  if (!ptr)
    return ec::invalid_table_slice_type;
  return ptr.unshared().deserialize(source);
}

void table_slice::append_column_to_index(size_type col,
                                         value_index& idx) const {
  for (size_type row = 0; row < rows(); ++row)
    idx.append(at(row, col), offset() + row);
}

table_slice_ptr make_table_slice(record_type layout, caf::actor_system& sys,
                                 caf::atom_value impl,
                                 table_slice::size_type rows) {
  if (impl == default_table_slice::class_id) {
    return caf::make_copy_on_write<default_table_slice>(std::move(layout));
  }
  using generic_fun = caf::runtime_settings_map::generic_function_pointer;
  using factory_fun = table_slice_ptr (*)(record_type, table_slice::size_type);
  auto val = sys.runtime_settings().get(impl);
  if (!caf::holds_alternative<generic_fun>(val)) {
    VAST_ERROR_ANON("table_slice",
                    "has no factory function for implementation key", impl);
    return table_slice_ptr{nullptr};
  }
  auto fun = reinterpret_cast<factory_fun>(caf::get<generic_fun>(val));
  return fun(std::move(layout), rows);
}

expected<std::vector<table_slice_ptr>>
make_random_table_slices(size_t num_slices, size_t slice_size,
                         record_type layout, id offset, size_t seed) {
  schema sc;
  sc.add(layout);
  format::test::reader src{seed, std::numeric_limits<uint64_t>::max(),
                           std::move(sc)};
  std::vector<table_slice_ptr> result;
  result.reserve(num_slices);
  default_table_slice_builder builder{std::move(layout)};
  for (size_t i = 0; i < num_slices; ++i) {
    if (auto e = src.read(builder, slice_size))
      return e;
    if (auto ptr = builder.finish(); ptr == nullptr) {
      return make_error(ec::unspecified, "finish failed");
    } else {
      result.emplace_back(std::move(ptr)).unshared().offset(offset);
      offset += slice_size;
    }
  }
  return result;
}

void intrusive_ptr_add_ref(const table_slice* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const table_slice* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

table_slice* intrusive_cow_ptr_unshare(table_slice*& ptr) {
  return caf::default_intrusive_cow_ptr_unshare(ptr);
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

caf::error inspect(caf::serializer& sink, table_slice_ptr& ptr) {
  return table_slice::serialize_ptr(sink, ptr);
}

caf::error inspect(caf::deserializer& source, table_slice_ptr& ptr) {
  return table_slice::deserialize_ptr(source, ptr);
}

} // namespace vast
