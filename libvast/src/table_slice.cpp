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

#include <unordered_map>

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/execution_unit.hpp>
#include <caf/sec.hpp>
#include <caf/serializer.hpp>
#include <caf/streambuf.hpp>
#include <caf/stream_deserializer.hpp>
#include <caf/sum_type.hpp>

#include "vast/chunk.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/format/test.hpp"
#include "vast/logger.hpp"
#include "vast/value.hpp"
#include "vast/value_index.hpp"

#include "vast/detail/byte_swap.hpp"
#include "vast/detail/overload.hpp"

namespace vast {

namespace {

std::unordered_map<caf::atom_value, table_slice_factory> factory_;

using size_type = table_slice::size_type;

auto cap (size_type pos, size_type num, size_type last) {
  return num == table_slice::npos ? last : std::min(last, pos + num);
}

} // namespace <anonymous>

table_slice::table_slice()
  : offset_(0),
    rows_(0),
    columns_(0) {
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

caf::error table_slice::load(chunk_ptr chunk) {
  VAST_ASSERT(chunk);
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::charbuf buf{data, chunk->size()};
  caf::stream_deserializer<caf::charbuf&> source{buf};
  return deserialize(source);
}

void table_slice::append_column_to_index(size_type col,
                                         value_index& idx) const {
  for (size_type row = 0; row < rows(); ++row)
    idx.append(at(row, col), offset() + row);
}

bool add_table_slice_factory(caf::atom_value id, table_slice_factory f) {
  if (factory_.count(id) > 0)
    return false;
  factory_.emplace(id, f);
  return true;
}

table_slice_factory get_table_slice_factory(caf::atom_value id) {
  auto i = factory_.find(id);
  return i != factory_.end() ? i->second : nullptr;
}

table_slice_ptr make_table_slice(caf::atom_value id) {
  if (id == default_table_slice::class_id)
    return caf::make_copy_on_write<default_table_slice>();
  if (auto f = get_table_slice_factory(id))
    return (*f)();
  return {};
}

table_slice_ptr make_table_slice(chunk_ptr chunk) {
  if (!chunk || chunk->size() < 32)
    return {};
  // Setup a CAF deserializer.
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::charbuf buf{data, chunk->size()};
  caf::stream_deserializer<caf::charbuf&> source{buf};
  // Deserialize the class ID and default-construct a table slice.
  caf::atom_value id;
  if (auto err = source(id)) {
    VAST_ERROR_ANON(__func__, "failed to deserialize table slice ID");
    return {};
  }
  auto result = make_table_slice(id);
  if (!result) {
    VAST_ERROR_ANON(__func__, "no table slice factory for:", to_string(id));
    return {};
  }
  // Deserialize the table slice base class.
  auto& x = result.unshared();
  if (auto err = source(x)) {
    VAST_ERROR_ANON(__func__,
                    "failed to deserialize table slice:", to_string(err));
    return {};
  }
  // Skip table slice data already processed.
  VAST_ASSERT(chunk->size() > buf.in_avail());
  auto header_size = chunk->size() - buf.in_avail();
  x.load(chunk->slice(header_size));
  return result;
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
  if (!ptr)
    return sink(caf::atom("NULL"));
  return caf::error::eval([&] { return sink(ptr->implementation_id(), *ptr); },
                          [&] { return ptr->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, table_slice_ptr& ptr) {
  caf::atom_value id;
  if (auto err = source(id))
    return err;
  if (id == caf::atom("NULL")) {
    ptr.reset();
    return caf::none;
  }
  ptr = make_table_slice(id);
  if (!ptr)
    return ec::invalid_table_slice_type;
  auto& x = ptr.unshared();
  return caf::error::eval([&] { return source(x); },
                          [&] { return x.deserialize(source); });
}

} // namespace vast
