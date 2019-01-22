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

#include "fixtures/table_slices.hpp"

#include <caf/all.hpp>

#include "vast/chunk.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/span.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/value_index.hpp"

using namespace vast;

namespace fixtures {

namespace {

} // namespace <anonymous>

table_slices::table_slices() : sink{sys, buf} {
  // Register factories.
  factory<table_slice>::initialize();
  // Define our test layout.
  layout = record_type{
    {"a", boolean_type{}},
    {"b", integer_type{}},
    {"c", count_type{}},
    {"d", real_type{}},
    {"e", timespan_type{}},
    {"f", timestamp_type{}},
    {"g", string_type{}},
    {"h", pattern_type{}},
    {"i", address_type{}},
    {"j", subnet_type{}},
    {"k", port_type{}},
    {"l", vector_type{count_type{}}},
    {"m", set_type{boolean_type{}}},
    {"n", map_type{count_type{}, boolean_type{}}},
  };
  // Initialize test data.
  auto rows = std::vector<std::string>{
    "[T, +7, 42, 4.2, 1337ms, 2018-12-24, \"foo\", /foo.*bar/, 127.0.0.1,"
    " 10.0.0.0/8, 80/tcp, [1, 2, 3], {T, F}, {1 -> T, 2 -> F, 3 -> T}]",
    "[F, -7, 43, 0.42, -1337ms, 2018-12-25, \"bar\", nil, ::1, 64:ff9b::/96,"
    " 53/udp, [], {}, {-}]",
  };
  for (auto& row : rows) {
    auto xs = unbox(to<data>(row));
    test_data.push_back(caf::get<vector>(xs));
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
  test_message_serialization();
  test_load_from_chunk();
  test_append_column_to_index();
}

caf::binary_deserializer table_slices::make_source() {
  return caf::binary_deserializer{sys, buf};
}

table_slice_ptr table_slices::make_slice() {
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
  CHECK_EQUAL(slice->rows(), 2u);
  CHECK_EQUAL(slice->columns(), layout.fields.size());
  for (size_t row = 0; row < slice->rows(); ++row)
    for (size_t col = 0; col < slice->columns(); ++col) {
      MESSAGE("checking value at (" << row << ',' << col << ')');
      CHECK_EQUAL(slice->at(row, col), at(row, col));
    }
}

void table_slices::test_equality() {
  MESSAGE(">> test equality");
  auto slice1 = make_slice();
  auto slice2 = make_slice();
  CHECK_EQUAL(*slice1, *slice2);
}

void table_slices::test_copy() {
  MESSAGE(">> test copy");
  auto slice1 = make_slice();
  table_slice_ptr slice2{slice1->copy(), false};
  CHECK_EQUAL(*slice1, *slice2);
}

void table_slices::test_manual_serialization() {
  MESSAGE(">> test manual serialization via inspect");
  MESSAGE("make slices");
  auto slice1 = make_slice();
  table_slice_ptr slice2;
  MESSAGE("save content of the first slice into the buffer");
  CHECK_EQUAL(inspect(sink, slice1), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(inspect(source, slice2), caf::none);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
  buf.clear();
}

void table_slices::test_smart_pointer_serialization() {
  MESSAGE(">> test smart pointer serialization");
  MESSAGE("make slices");
  auto slice1 = make_slice();
  table_slice_ptr slice2;
  MESSAGE("save content of the first slice into the buffer");
  CHECK_EQUAL(sink(slice1), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(source(slice2), caf::none);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
  buf.clear();
}

void table_slices::test_message_serialization() {
  MESSAGE(">> test message serialization");
  MESSAGE("make slices");
  auto slice1 = caf::make_message(make_slice());
  caf::message slice2;
  MESSAGE("save content of the first slice into the buffer");
  CHECK_EQUAL(sink(slice1), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(source(slice2), caf::none);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE(slice2.match_elements<table_slice_ptr>());
  CHECK_EQUAL(*slice1.get_as<table_slice_ptr>(0),
              *slice2.get_as<table_slice_ptr>(0));
  CHECK_EQUAL(slice2.get_as<table_slice_ptr>(0)->implementation_id(),
              builder->implementation_id());
  buf.clear();
}

void table_slices::test_load_from_chunk() {
  MESSAGE(">> test load from chunk");
  auto slice1 = make_slice();
  CHECK_EQUAL(sink(slice1), caf::none);
  auto chk = chunk::make(make_const_byte_span(buf));
  auto slice2 = factory<table_slice>::traits::make(chk);
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
  buf.clear();
}

void table_slices::test_append_column_to_index() {
  MESSAGE(">> test append_column_to_index");
  auto idx = factory<value_index>::make(integer_type{});
  REQUIRE_NOT_EQUAL(idx, nullptr);
  auto slice = make_slice();
  slice->append_column_to_index(1, *idx);
  CHECK_EQUAL(idx->offset(), 2u);
  constexpr auto less = relational_operator::less;
  CHECK_EQUAL(unbox(idx->lookup(less, make_view(3))), make_ids({1}));
}

} // namespace fixtures
