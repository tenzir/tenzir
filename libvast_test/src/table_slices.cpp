//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "fixtures/table_slices.hpp"

#include "vast/chunk.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/detail/append.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/format/test.hpp"
#include "vast/operator.hpp"
#include "vast/value_index.hpp"
#include "vast/value_index_factory.hpp"

#include <span>

using namespace std::string_literals;
using namespace vast;

namespace vast {

/// Constructs table slices filled with random content for testing purposes.
/// @param num_slices The number of table slices to generate.
/// @param slice_size The number of rows per table slices.
/// @param schema The schema of the table slice.
/// @param offset The offset of the first table slize.
/// @param seed The seed value for initializing the random-number generator.
/// @returns a list of randomnly filled table slices or an error.
/// @relates table_slice
caf::expected<std::vector<table_slice>>
make_random_table_slices(size_t num_slices, size_t slice_size, type schema,
                         id offset, size_t seed) {
  module mo;
  mo.add(schema);
  // We have no access to the actor system, so we can only pick the default
  // table slice type here. This ignores any user-defined overrides. However,
  // this function is only meant for testing anyways.
  caf::settings opts;
  caf::put(opts, "vast.import.test.seed", seed);
  caf::put(opts, "vast.import.max-events", std::numeric_limits<size_t>::max());
  format::test::reader src{std::move(opts), nullptr};
  REQUIRE_EQUAL(src.module(std::move(mo)), caf::error{});
  std::vector<table_slice> result;
  auto add_slice = [&](table_slice slice) {
    slice.offset(offset);
    offset += slice.rows();
    result.emplace_back(std::move(slice));
  };
  result.reserve(num_slices);
  if (auto err = src.read(num_slices * slice_size, slice_size, add_slice).first)
    return err;
  return result;
}

/// Converts the table slice into a 2-D matrix in row-major order such that
/// each row represents an event.
/// @param slice The table slice to convert.
/// @param first_row An offset to the first row to consider.
/// @param num_rows Then number of rows to consider. (0 = all rows)
/// @returns a 2-D matrix of data instances corresponding to *slice*.
/// @requires first_row < slice.rows()
/// @requires num_rows <= slice.rows() - first_row
/// @note This function exists primarily for unit testing because it performs
/// excessive memory allocations.
std::vector<std::vector<data>>
make_data(const table_slice& slice, size_t first_row, size_t num_rows) {
  VAST_ASSERT(first_row < slice.rows());
  VAST_ASSERT(num_rows <= slice.rows() - first_row);
  if (num_rows == 0)
    num_rows = slice.rows() - first_row;
  std::vector<std::vector<data>> result;
  result.reserve(num_rows);
  auto fl = flatten(caf::get<record_type>(slice.schema()));
  for (size_t i = 0; i < num_rows; ++i) {
    std::vector<data> xs;
    xs.reserve(slice.columns());
    for (size_t j = 0; j < slice.columns(); ++j)
      xs.emplace_back(
        materialize(slice.at(first_row + i, j, fl.field(j).type)));
    result.push_back(std::move(xs));
  }
  return result;
}

std::vector<std::vector<data>>
make_data(const std::vector<table_slice>& slices) {
  std::vector<std::vector<data>> result;
  result.reserve(rows(slices));
  for (auto& slice : slices)
    detail::append(result, make_data(slice));
  return result;
}

} // namespace vast

namespace fixtures {

table_slices::table_slices(std::string_view suite)
  : fixtures::deterministic_actor_system_and_events(suite) {
  // A bunch of test data for nested type combinations.
  // clang-format off
  auto test_lists = ""s
    + ", [true]"s // va
    + ", [+7]"s // vb
    + ", [42]"s // vc
    + ", [4.2]"s // vd
    + ", [1337ms]"s // ve
    + ", [2018-12-24]"s // vf
    + ", [\"foo\"]"s // vg
    /* + ", [/foo.*bar/]"s // vh */
    + ", [127.0.0.1]"s // vi
    + ", [10.0.0.0/8]"s // vj
    // + ", [[1, 2, 3]]"s // vl
    // + ", [{1 -> true, 2 -> false, 3 -> true}]"s // vm
    ;

  auto test_collections
    = test_lists
    ;
  // clang-format on
  // Initialize test data.
  auto rows = std::vector<std::string>{
    "[true, +7, 42, 4.2, 1337ms, 2018-12-24, \"foo\", 127.0.0.1,"
    " 10.0.0.0/8, [1, 2, 3]"
      + test_collections + ", \"aas\"]",
    "[false, -7, 43, 0.42, -1337ms, 2018-12-25, \"bar\", ::1, "
    "64:ff9b::/96,"
    " []"
      + test_collections + ", \"aas\"]",
  };
  for (auto& row : rows) {
    auto xs = unbox(to<data>(row));
    test_data.push_back(caf::get<list>(xs));
  }
}

void table_slices::run() {
  if (builder == nullptr)
    FAIL("no valid builder found; missing fixture initialization?");
  test_add();
  test_equality();
  test_copy();
  test_manual_serialization();
  test_smart_pointer_serialization();
  test_append_column_to_index();
}

caf::binary_serializer table_slices::make_sink() {
  buf.clear();
  return caf::binary_serializer{sys, buf};
}

table_slice table_slices::make_slice() {
  for (auto& xs : test_data)
    for (auto& x : xs)
      if (!builder->add(make_view(x)))
        FAIL("builder failed to add element");
  return builder->finish();
}

vast::data_view table_slices::at(size_t row, size_t col) const {
  VAST_ASSERT(row < test_data.size());
  VAST_ASSERT(col < test_data[row].size());
  return make_view(test_data[row][col]);
}

void table_slices::test_add() {
  MESSAGE(">> test table_slice_builder::add");
  auto slice = make_slice();
  CHECK_EQUAL(slice.rows(), 2u);
  auto flat_schema = flatten(caf::get<record_type>(schema));
  CHECK_EQUAL(slice.columns(), flat_schema.num_fields());
  for (size_t row = 0; row < slice.rows(); ++row)
    for (size_t col = 0; col < slice.columns(); ++col) {
      MESSAGE("checking value at (" << row << ',' << col << ')');
      CHECK_EQUAL(materialize(slice.at(row, col)), materialize(at(row, col)));
    }
}

void table_slices::test_equality() {
  MESSAGE(">> test equality");
  auto slice1 = make_slice();
  auto slice2 = make_slice();
  CHECK_EQUAL(slice1, slice2);
}

void table_slices::test_copy() {
  MESSAGE(">> test copy");
  auto slice1 = make_slice();
  table_slice slice2{slice1};
  CHECK_EQUAL(slice1, slice2);
}

void table_slices::test_manual_serialization() {
  MESSAGE(">> test manual serialization via inspect");
  MESSAGE("make slices");
  auto slice1 = make_slice();
  table_slice slice2;
  MESSAGE("save content of the first slice into the buffer");
  auto sink = make_sink();
  CHECK(inspect(sink, slice1));
  MESSAGE("load content for the second slice from the buffer");
  CHECK_EQUAL(vast::detail::legacy_deserialize(buf, slice2), true);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE_NOT_EQUAL(slice2.encoding(), table_slice_encoding::none);
  CHECK_EQUAL(slice1, slice2);
}

void table_slices::test_smart_pointer_serialization() {
  MESSAGE(">> test smart pointer serialization");
  MESSAGE("make slices");
  auto slice1 = make_slice();
  table_slice slice2;
  MESSAGE("save content of the first slice into the buffer");
  auto sink = make_sink();
  CHECK(sink.apply(slice1));
  MESSAGE("load content for the second slice from the buffer");
  CHECK_EQUAL(vast::detail::legacy_deserialize(buf, slice2), true);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE_NOT_EQUAL(slice2.encoding(), table_slice_encoding::none);
  CHECK_EQUAL(slice1, slice2);
}

void table_slices::test_append_column_to_index() {
  MESSAGE(">> test append_column_to_index");
  auto idx = factory<value_index>::make(type{int64_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(idx, nullptr);
  auto slice = make_slice();
  slice.offset(0);
  slice.append_column_to_index(1, *idx);
  CHECK_EQUAL(idx->offset(), 2u);
  constexpr auto less = relational_operator::less;
  CHECK_EQUAL(unbox(idx->lookup(less, make_view(int64_t{3}))), make_ids({1}));
}

} // namespace fixtures
