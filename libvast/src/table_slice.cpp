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

#include "vast/caf_table_slice.hpp"
#include "vast/caf_table_slice_builder.hpp"
#include "vast/chunk.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/factory.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/value_index.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/execution_unit.hpp>
#include <caf/sec.hpp>
#include <caf/serializer.hpp>
#include <caf/sum_type.hpp>

#include <unordered_map>

namespace vast {

namespace {

using size_type = table_slice::size_type;

} // namespace <anonymous>

// -- constructors, destructors, and assignment operators ----------------------

table_slice::table_slice() noexcept = default;

table_slice::table_slice(const table_slice& other) noexcept = default;

table_slice& table_slice::operator=(const table_slice&) noexcept = default;

table_slice::table_slice(table_slice_header header) noexcept
  : header_{std::move(header)} {
  ++instance_count_;
}

table_slice::~table_slice() noexcept {
  --instance_count_;
}

// -- persistence --------------------------------------------------------------

caf::error table_slice::load(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::binary_deserializer source{nullptr, data, chunk->size()};
  return deserialize(source);
}

// -- visitation ---------------------------------------------------------------

void table_slice::append_column_to_index(size_type col,
                                         value_index& idx) const {
  for (size_type row = 0; row < rows(); ++row)
    idx.append(at(row, col), offset() + row);
}

// -- properties ---------------------------------------------------------------

const record_type& table_slice::layout() const noexcept {
  return header_.layout;
}

size_type table_slice::rows() const noexcept {
  return header_.rows;
}

size_type table_slice::columns() const noexcept {
  return header_.layout.fields.size();
}

id table_slice::offset() const noexcept {
  return header_.offset;
}

void table_slice::offset(id offset) noexcept {
  header_.offset = offset;
}

int table_slice::instances() {
  return instance_count_;
}

// -- comparison operators -----------------------------------------------------

bool operator==(const table_slice& x, const table_slice& y) {
  if (&x == &y)
    return true;
  if (x.rows() != y.rows() || x.columns() != y.columns()
      || x.layout() != y.layout())
    return false;
  for (size_t row = 0; row < x.rows(); ++row)
    for (size_t col = 0; col < x.columns(); ++col)
      if (x.at(row, col) != y.at(row, col))
        return false;
  return true;
}

bool operator!=(const table_slice& x, const table_slice& y) {
  return !(x == y);
}

// -- concepts -----------------------------------------------------------------

caf::error inspect(caf::serializer& sink, table_slice_ptr& ptr) {
  if (!ptr)
    return sink(caf::atom("NULL"));
  return caf::error::eval(
    [&] { return sink(ptr->implementation_id()); },
    [&] { return sink(ptr->layout(), ptr->rows(), ptr->offset()); },
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
  table_slice_header header;
  if (auto err = source(header.layout, header.rows, header.offset))
    return err;
  ptr = factory<table_slice>::make(id, std::move(header));
  if (!ptr)
    return ec::invalid_table_slice_type;
  return ptr.unshared().deserialize(source);
}

// TODO: this function will boil down to accessing the chunk inside the table
// slice and then calling GetTableSlice(buf). But until we touch the table
// slice internals, we use this helper.
caf::expected<flatbuffers::Offset<fbs::table_slice_buffer::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, table_slice_ptr x) {
  // This local builder instance will vanish once we can access the underlying
  // chunk of a table slice.
  flatbuffers::FlatBufferBuilder local_builder;
  std::vector<char> layout_buffer;
  caf::binary_serializer sink1{nullptr, layout_buffer};
  if (auto error = sink1(x->layout()))
    return error;
  std::vector<char> data_buffer;
  caf::binary_serializer sink2{nullptr, data_buffer};
  if (auto error = sink2(x))
    return error;
  auto transform = [](caf::atom_value x) -> caf::expected<fbs::Encoding> {
    if (x == caf::atom("caf"))
      return fbs::Encoding::CAF;
    if (x == caf::atom("arrow"))
      return fbs::Encoding::Arrow;
    if (x == caf::atom("msgpack"))
      return fbs::Encoding::MessagePack;
    return make_error(ec::unspecified, "unsupported table slice type", x);
  };
  auto encoding = transform(x->implementation_id());
  if (!encoding)
    return encoding.error();
  auto layout_ptr = reinterpret_cast<const uint8_t*>(layout_buffer.data());
  auto layout = local_builder.CreateVector(layout_ptr, layout_buffer.size());
  auto data_ptr = reinterpret_cast<const uint8_t*>(data_buffer.data());
  auto data = local_builder.CreateVector(data_ptr, data_buffer.size());
  fbs::table_slice::v0Builder table_slice_builder{local_builder};
  table_slice_builder.add_offset(x->offset());
  table_slice_builder.add_rows(x->rows());
  table_slice_builder.add_layout(layout);
  table_slice_builder.add_encoding(*encoding);
  table_slice_builder.add_data(data);
  auto flat_slice = table_slice_builder.Finish();
  local_builder.Finish(flat_slice);
  auto buffer = span<const uint8_t>{local_builder.GetBufferPointer(),
                                    local_builder.GetSize()};
  // This is the only code that will remain. All the stuff above will move into
  // the respective table slice builders.
  auto bytes = builder.CreateVector(buffer.data(), buffer.size());
  fbs::table_slice_buffer::v0Builder table_slice_buffer_builder{builder};
  table_slice_buffer_builder.add_data(bytes);
  return table_slice_buffer_builder.Finish();
}

// TODO: The dual to the note above applies here.
caf::error unpack(const fbs::table_slice::v0& x, table_slice_ptr& y) {
  auto ptr = reinterpret_cast<const char*>(x.data()->Data());
  caf::binary_deserializer source{nullptr, ptr, x.data()->size()};
  return source(y);
}

// -- intrusive_ptr facade -----------------------------------------------------

void intrusive_ptr_add_ref(const table_slice* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const table_slice* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

table_slice* intrusive_cow_ptr_unshare(table_slice*& ptr) {
  return caf::default_intrusive_cow_ptr_unshare(ptr);
}

// -- operators ----------------------------------------------------------------

void select(std::vector<table_slice_ptr>& result, const table_slice_ptr& xs,
            const ids& selection) {
  VAST_ASSERT(xs != nullptr);
  auto xs_ids = make_ids({{xs->offset(), xs->offset() + xs->rows()}});
  auto intersection = selection & xs_ids;
  auto intersection_rank = rank(intersection);
  // Do no rows qualify?
  if (intersection_rank == 0)
    return;
  // Do all rows qualify?
  if (rank(xs_ids) == intersection_rank) {
    result.emplace_back(xs);
    return;
  }
  // Start slicing and dicing.
  auto impl = xs->implementation_id();
  auto builder = factory<table_slice_builder>::make(impl, xs->layout());
  if (builder == nullptr) {
    VAST_ERROR(__func__, "failed to get a table slice builder for", impl);
    return;
  }
  id last_offset = xs->offset();
  auto push_slice = [&] {
    if (builder->rows() == 0)
      return;
    auto slice = builder->finish();
    if (slice == nullptr) {
      VAST_WARNING(__func__, "got an empty slice");
      return;
    }
    slice.unshared().offset(last_offset);
    result.emplace_back(std::move(slice));
  };
  auto last_id = last_offset - 1;
  for (auto id : select(intersection)) {
    // Finish last slice when hitting non-consecutive IDs.
    if (last_id + 1 != id) {
      push_slice();
      last_offset = id;
      last_id = id;
    } else {
      ++last_id;
    }
    VAST_ASSERT(id >= xs->offset());
    auto row = id - xs->offset();
    VAST_ASSERT(row < xs->rows());
    for (size_t column = 0; column < xs->columns(); ++column) {
      auto cell_value = xs->at(row, column);
      if (!builder->add(cell_value)) {
        VAST_ERROR(__func__, "failed to add data at column", column, "in row",
                   row, "to the builder:", cell_value);
        return;
      }
    }
  }
  push_slice();
}

std::vector<table_slice_ptr> select(const table_slice_ptr& xs,
                                    const ids& selection) {
  std::vector<table_slice_ptr> result;
  select(result, xs, selection);
  return result;
}

table_slice_ptr truncate(const table_slice_ptr& slice, size_t num_rows) {
  VAST_ASSERT(slice != nullptr);
  VAST_ASSERT(num_rows > 0);
  if (slice->rows() <= num_rows)
    return slice;
  auto selection = make_ids({{slice->offset(), slice->offset() + num_rows}});
  auto xs = select(slice, selection);
  VAST_ASSERT(xs.size() == 1);
  return std::move(xs.back());
}

std::pair<table_slice_ptr, table_slice_ptr> split(const table_slice_ptr& slice,
                                                  size_t partition_point) {
  VAST_ASSERT(slice != nullptr);
  if (partition_point == 0)
    return {nullptr, slice};
  if (partition_point >= slice->rows())
    return {slice, nullptr};
  auto first = slice->offset();
  auto mid = first + partition_point;
  auto last = first + slice->rows();
  // Create first table slice.
  auto xs = select(slice, make_ids({{first, mid}}));
  VAST_ASSERT(xs.size() == 1);
  // Create second table slice.
  select(xs, slice, make_ids({{mid, last}}));
  VAST_ASSERT(xs.size() == 2);
  return {std::move(xs.front()), std::move(xs.back())};
}

uint64_t rows(const std::vector<table_slice_ptr>& slices) {
  auto result = uint64_t{0};
  for (auto& slice : slices)
    result += slice->rows();
  return result;
}

namespace {

struct row_evaluator {
  row_evaluator(const table_slice& slice, size_t row)
    : slice_{slice}, row_{row} {
    // nop
  }

  template <class T>
  bool operator()(const data& d, const T& x) {
    return (*this)(x, d);
  }

  template <class T, class U>
  bool operator()(const T&, const U&) {
    return false;
  }

  bool operator()(caf::none_t) {
    return false;
  }

  bool operator()(const conjunction& c) {
    for (auto& op : c)
      if (!caf::visit(*this, op))
        return false;
    return true;
  }

  bool operator()(const disjunction& d) {
    for (auto& op : d)
      if (caf::visit(*this, op))
        return true;
    return false;
  }

  bool operator()(const negation& n) {
    return !caf::visit(*this, n.expr());
  }

  bool operator()(const predicate& p) {
    op_ = p.op;
    return caf::visit(*this, p.lhs, p.rhs);
  }

  bool operator()(const attribute_extractor& e, const data& d) {
    // TODO: Transform this AST node into a constant-time lookup node (e.g.,
    // data_extractor). It's not necessary to iterate over the schema for every
    // row; this should happen upfront.
    if (e.attr == atom::type_v)
      return evaluate(slice_.layout().name(), op_, d);
    if (e.attr == atom::timestamp_v) {
      for (size_t col = 0; col < slice_.layout().fields.size(); ++col) {
        auto& field = slice_.layout().fields[col];
        if (has_attribute(field.type, "timestamp")) {
          if (!caf::holds_alternative<time_type>(field.type)) {
            VAST_WARNING_ANON("got timestamp attribute for non-time type");
            return false;
          }
        }
        auto lhs = to_canonical(field.type, slice_.at(row_, col));
        auto rhs = make_view(d);
        return evaluate_view(lhs, op_, rhs);
      }
    }
    return false;
  }

  bool operator()(const type_extractor&, const data&) {
    die("type extractor should have been resolved at this point");
  }

  bool operator()(const field_extractor&, const data&) {
    die("field extractor should have been resolved at this point");
  }

  bool operator()(const data_extractor& e, const data& d) {
    VAST_ASSERT(e.offset.size() == 1);
    if (e.type != slice_.layout()) // TODO: make this a precondition instead.
      return false;
    auto col = e.offset[0];
    auto& field = slice_.layout().fields[col];
    auto lhs = to_canonical(field.type, slice_.at(row_, col));
    auto rhs = make_data_view(d);
    return evaluate_view(lhs, op_, rhs);
  }

  const table_slice& slice_;
  size_t row_;
  relational_operator op_;
};

} // namespace

ids evaluate(const expression& expr, const table_slice& slice) {
  // TODO: switch to a column-based evaluation strategy where it makes sense.
  ids result;
  result.append(false, slice.offset());
  for (size_t row = 0; row != slice.rows(); ++row) {
    auto x = caf::visit(row_evaluator{slice, row}, expr);
    result.append_bit(x);
  }
  return result;
}

} // namespace vast
