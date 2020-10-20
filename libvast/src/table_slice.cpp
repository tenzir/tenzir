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

// -- v1 includes --------------------------------------------------------------

#include "vast/table_slice.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/msgpack_table_slice.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/table_slice_row.hpp"
#include "vast/table_slice_visit.hpp"
#include "vast/value_index.hpp"

#include <utility>

// -- v0 includes --------------------------------------------------------------

#include "vast/chunk.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/append.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/factory.hpp"
#include "vast/format/test.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
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

// -- constructors, destructors, and assignment operators ----------------------

table_slice::table_slice() noexcept {
  ++num_instances_;
}

table_slice::~table_slice() noexcept {
  --num_instances_;
}

table_slice::table_slice(const table_slice& other) noexcept
  : chunk_{other.chunk_}, offset_{other.offset_}, pimpl_{other.pimpl_} {
  ++num_instances_;
}

table_slice& table_slice::operator=(const table_slice& rhs) noexcept {
  chunk_ = rhs.chunk_;
  offset_ = rhs.offset_;
  pimpl_ = rhs.pimpl_;
  ++num_instances_;
  return *this;
}

table_slice::table_slice(table_slice&& other) noexcept
  : chunk_{std::exchange(other.chunk_, {})},
    offset_{std::exchange(other.offset_, invalid_id)},
    pimpl_{std::move(other.pimpl_)} {
  other.pimpl_.invalid = nullptr;
}

table_slice& table_slice::operator=(table_slice&& rhs) noexcept {
  chunk_ = std::exchange(rhs.chunk_, {});
  offset_ = std::exchange(rhs.offset_, invalid_id);
  pimpl_ = std::move(rhs.pimpl_);
  rhs.pimpl_.invalid = nullptr;
  return *this;
}

// -- comparison operators -----------------------------------------------------

bool operator==(const table_slice& lhs, const table_slice& rhs) {
  if (&lhs == &rhs)
    return true;
  if (lhs.rows() != rhs.rows() || lhs.columns() != rhs.columns()
      || lhs.layout() != rhs.layout())
    return false;
  for (size_t row = 0; row < lhs.rows(); ++row)
    for (size_t col = 0; col < lhs.columns(); ++col)
      if (lhs.at(row, col) != rhs.at(row, col))
        return false;
  return true;
}

// -- properties: encoding -----------------------------------------------------

/// @returns The encoding of the table slice.
table_slice_encoding table_slice::encoding() const noexcept {
  return visit(detail::overload{
                 []() noexcept { return table_slice_encoding::invalid; },
                 [](const fbs::table_slice::msgpack::v0&) noexcept {
                   return table_slice_encoding::msgpack;
                 },
                 [](const fbs::table_slice::arrow::v0&) noexcept {
                   return table_slice_encoding::arrow;
                 },
               },
               *this);
}

// -- properties: offset -------------------------------------------------------

id table_slice::offset() const noexcept {
  return offset_;
}

/// Sets the offset in the ID space.
void table_slice::offset(id offset) noexcept {
  // It is usually an error to set the offset when the table slice is shared.
  VAST_ASSERT(chunk());
  VAST_ASSERT(chunk()->unique());
  offset_ = std::move(offset);
}

// -- properties: rows ---------------------------------------------------------

table_slice::size_type table_slice::rows() const noexcept {
  return visit(detail::overload{
                 []() noexcept -> size_type { return {}; },
                 [&](const fbs::table_slice::msgpack::v0&) noexcept {
                   return pimpl_.msgpack->rows();
                 },
                 [&](const fbs::table_slice::arrow::v0&) noexcept {
                   return pimpl_.arrow->rows();
                 },
               },
               *this);
}

// -- properties: columns ------------------------------------------------------

table_slice::size_type table_slice::columns() const noexcept {
  return visit(detail::overload{
                 []() noexcept -> size_type { return {}; },
                 [&](const fbs::table_slice::msgpack::v0&) noexcept {
                   return pimpl_.msgpack->columns();
                 },
                 [&](const fbs::table_slice::arrow::v0&) noexcept {
                   return pimpl_.arrow->columns();
                 },
               },
               *this);
}

caf::optional<table_slice_column>
table_slice::column(std::string_view name) const& {
  for (size_type column = 0; column < layout().fields.size(); ++column)
    if (layout().fields[column].name == name)
      return table_slice_column{*this, column};
  return caf::none;
}

caf::optional<table_slice_column>
table_slice::column(std::string_view name) && {
  for (size_type column = 0; column < layout().fields.size(); ++column)
    if (layout().fields[column].name == name)
      return table_slice_column{*this, column};
  return caf::none;
}

// -- properties: layout -------------------------------------------------------

const record_type& table_slice::layout() const noexcept {
  return *visit(detail::overload{
                  []() noexcept {
                    static const auto result = record_type{};
                    return &result;
                  },
                  [&](const fbs::table_slice::msgpack::v0&) noexcept {
                    return &pimpl_.msgpack->layout();
                  },
                  [&](const fbs::table_slice::arrow::v0&) noexcept {
                    return &pimpl_.arrow->layout();
                  },
                },
                *this);
}

// -- properties: data access --------------------------------------------------

data_view table_slice::at(size_type row, size_type column) const {
  VAST_ASSERT(row < rows());
  VAST_ASSERT(column < columns());
  return visit(detail::overload{
                 []() noexcept -> data_view {
                   // The preconditions imply that this handler can never be
                   // called.
                   die("logic error: invalid table slices cannot be accessed");
                 },
                 [&](const fbs::table_slice::msgpack::v0&) noexcept {
                   return pimpl_.msgpack->at(row, column);
                 },
                 [&](const fbs::table_slice::arrow::v0&) noexcept {
                   return pimpl_.arrow->at(row, column);
                 },
               },
               *this);
}

void table_slice::append_column_to_index(size_type column,
                                         value_index& idx) const {
  visit(detail::overload{
          []() noexcept {
            // An invalid slice cannot be added to a value index, so this is
            // just a nop.
          },
          [&](const fbs::table_slice::msgpack::v0&) {
            pimpl_.msgpack->append_column_to_index(offset(), column, idx);
          },
          [&](const fbs::table_slice::arrow::v0&) {
            pimpl_.arrow->append_column_to_index(offset(), column, idx);
          },
        },
        *this);
}

// -- type introspection -------------------------------------------------------

const chunk_ptr& table_slice::chunk() const noexcept {
  return chunk_;
}

size_t table_slice::instances() noexcept {
  return table_slice::num_instances_;
}

caf::error unpack(const fbs::TableSliceBuffer& source, table_slice& dest) {
  auto chunk = chunk::make(std::vector{
    source.data()->Data(), source.data()->Data() + source.data()->size()});
  dest = table_slice{std::move(chunk)};
  return caf::none;
}

// -- implementation details ---------------------------------------------------

table_slice::table_slice(chunk_ptr&& chunk) noexcept
  : chunk_{std::move(chunk)} {
  VAST_ASSERT(chunk_);
  VAST_ASSERT(chunk_->unique());
  visit(detail::overload{
          []() noexcept {
            // nop
          },
          [this](const fbs::table_slice::arrow::v0& slice) noexcept {
            pimpl_.arrow = new arrow_table_slice{slice};
            chunk_->add_deletion_step([this] { delete pimpl_.arrow; });
          },
          [this](const fbs::table_slice::msgpack::v0& slice) noexcept {
            pimpl_.msgpack = new msgpack_table_slice{slice};
            chunk_->add_deletion_step([this] { delete pimpl_.msgpack; });
          },
        },
        *this);
}

// -- row operations -----------------------------------------------------------

void select(std::vector<table_slice>& result, table_slice slice,
            const ids& selection) {
  auto slice_ids = make_ids({{slice.offset(), slice.offset() + slice.rows()}});
  auto intersection = selection & slice_ids;
  auto intersection_rank = rank(intersection);
  // Do no rows qualify?
  if (intersection_rank == 0)
    return;
  // Do all rows qualify?
  if (rank(slice_ids) == intersection_rank) {
    result.emplace_back(std::move(slice));
    return;
  }
  // Start slicing and dicing.
  auto builder
    = factory<table_slice_builder>::make(slice.encoding(), slice.layout());
  if (builder == nullptr) {
    VAST_ERROR(__func__, "failed to get a table slice builder for",
               slice.encoding());
    return;
  }
  id last_offset = slice.offset();
  auto push_slice = [&] {
    if (builder->rows() == 0)
      return;
    auto slice = builder->finish();
    if (slice.rows() == 0) {
      VAST_WARNING(__func__, "got an empty slice");
      return;
    }
    slice.offset(last_offset);
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
    VAST_ASSERT(id >= slice.offset());
    auto row = id - slice.offset();
    VAST_ASSERT(row < slice.rows());
    for (size_t column = 0; column < slice.columns(); ++column) {
      auto cell_value = slice.at(row, column);
      if (!builder->add(cell_value)) {
        VAST_ERROR(__func__, "failed to add data at column", column, "in row",
                   row, "to the builder:", cell_value);
        return;
      }
    }
  }
  push_slice();
}

std::vector<table_slice> select(table_slice slice, const ids& selection) {
  std::vector<table_slice> result;
  select(result, std::move(slice), selection);
  return result;
}

table_slice truncate(table_slice slice, table_slice::size_type num_rows) {
  if (num_rows == 0)
    return {};
  if (slice.rows() <= num_rows)
    return slice;
  auto selection = make_ids({{slice.offset(), slice.offset() + num_rows}});
  auto xs = select(std::move(slice), selection);
  VAST_ASSERT(xs.size() == 1);
  return std::move(xs.back());
}

std::pair<table_slice, table_slice>
split(table_slice slice, table_slice::size_type partition_point) {
  if (partition_point == 0)
    return {{}, std::move(slice)};
  if (partition_point >= slice.rows())
    return {std::move(slice), {}};
  auto first = slice.offset();
  auto mid = first + partition_point;
  auto last = first + slice.rows();
  // Create first table slice.
  auto xs = select(std::move(slice), make_ids({{first, mid}}));
  VAST_ASSERT(xs.size() == 1);
  // Create second table slice.
  select(xs, slice, make_ids({{mid, last}}));
  VAST_ASSERT(xs.size() == 2);
  return {std::move(xs.front()), std::move(xs.back())};
}

table_slice::size_type rows(const std::vector<table_slice>& slices) {
  return std::transform_reduce(slices.begin(), slices.end(),
                               table_slice::size_type{}, std::plus<>{},
                               [](auto&& slice) { return slice.rows(); });
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
