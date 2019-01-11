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
#include "vast/span.hpp"
#include "vast/value_index.hpp"

using namespace vast;

namespace fixtures {

namespace {

} // namespace <anonymous>

table_slices::table_slices() : sink{sys, buf} {
  // Define our test type.
  layout = record_type{
    {"a", integer_type{}},
    {"b", string_type{}},
    {"c", real_type{}}
  };
  // Initialize test data.
  test_data.assign({
    triple{1, "abc", 1.2},
    triple{2, "def", 2.1},
    triple{3, "ghi", 42.},
    triple{4, "jkl", .42},
    triple{5, "mno", 123},
  });
  for (auto& x : test_data)
    test_values.emplace_back(value::make(make_vector(x), layout));
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
  for (auto& x : test_data)
    std::apply(
      [&](auto... xs) {
        if ((!builder->add(make_view(xs)) || ...))
          FAIL("builder failed to add element");
      },
      x);
  return builder->finish();
}

std::vector<value> table_slices::select(size_t from, size_t num) {
  return {test_values.begin() + from, test_values.begin() + (from + num)};
}

void table_slices::test_add() {
  MESSAGE(">> test table_slice_builder::add");
  MESSAGE("1st row");
  auto foo = "foo"s;
  auto bar = "foo"s;
  CHECK(builder->add(make_view(42)));
  CHECK(!builder->add(make_view(true))); // wrong type
  CHECK(builder->add(make_view(foo)));
  CHECK(builder->add(make_view(4.2)));
  MESSAGE("2nd row");
  CHECK(builder->add(make_view(43)));
  CHECK(builder->add(make_view(bar)));
  CHECK(builder->add(make_view(4.3)));
  MESSAGE("finish");
  auto slice = builder->finish();
  CHECK_EQUAL(slice->rows(), 2u);
  CHECK_EQUAL(slice->columns(), 3u);
  CHECK_EQUAL(slice->at(0, 1), make_view(foo));
  CHECK_EQUAL(slice->at(1, 2), make_view(4.3));
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
  auto slice2 = make_table_slice(chk);
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
  buf.clear();
}

void table_slices::test_append_column_to_index() {
  MESSAGE(">> test append_column_to_index");
  auto idx = value_index::make(integer_type{});
  REQUIRE(idx != nullptr);
  auto slice = make_slice();
  slice->append_column_to_index(0, *idx);
  CHECK_EQUAL(idx->offset(), 5u);
  constexpr auto less = relational_operator::less;
  CHECK_EQUAL(unbox(idx->lookup(less, vast::make_view(3))),
              make_ids({0, 1}, 5));
}

} // namespace fixtures
