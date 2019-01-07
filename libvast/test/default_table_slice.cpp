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

#include <string>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/make_copy_on_write.hpp>

#include "vast/default_table_slice.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/subset.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/value.hpp"
#include "vast/view.hpp"

#define SUITE default_table_slice
#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

namespace {

class rebranded_table_slice : public default_table_slice {
public:
  using super = default_table_slice;

  using super::super;

  rebranded_table_slice(const default_table_slice& other) : super(other) {
    // nop
  }

  caf::atom_value implementation_id() const noexcept override {
    return caf::atom("test");
  }
};

table_slice_ptr rebranded_slice_factory(record_type layout) {
  return caf::make_copy_on_write<rebranded_table_slice>(std::move(layout));
}

struct fixture : fixtures::deterministic_actor_system {
  record_type layout = record_type{
    {"a", integer_type{}},
    {"b", string_type{}},
    {"c", real_type{}}
  };

  table_slice_builder_ptr builder = default_table_slice::make_builder(layout);

  using tup = std::tuple<integer, std::string, real>;

  std::vector<tup> test_data;

  std::vector<value> test_values;

  std::vector<char> buf;

  caf::binary_serializer sink;

  auto make_source() {
    return caf::binary_deserializer{sys, buf};
  }

  fixture() : sink(sys, buf) {
    REQUIRE_NOT_EQUAL(builder, nullptr);
    test_data.assign({
      tup{1, "abc", 1.2},
      tup{2, "def", 2.1},
      tup{3, "ghi", 42.},
      tup{4, "jkl", .42}
    });
    for (auto& x : test_data)
      test_values.emplace_back(value::make(make_vector(x), layout));
  }

  auto make_slice() {
    for (auto& x : test_data)
      std::apply(
        [&](auto... xs) {
          if ((!builder->add(make_view(xs)) || ...))
            FAIL("builder failed to add element");
        },
        x);
    return builder->finish();
  }

  table_slice_ptr make_rebranded_slice() {
    auto from = make_slice();
    auto& dref = static_cast<const default_table_slice&>(*from);
    return caf::make_copy_on_write<rebranded_table_slice>(dref);
  }

  std::vector<value> select(size_t from, size_t num) {
    return {test_values.begin() + from, test_values.begin() + (from + num)};
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(default_table_slice_tests, fixture)

TEST(add) {
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

TEST(rows to values) {
  auto slice = make_slice();
  CHECK_EQUAL(subset(*slice), test_values);
  CHECK_EQUAL(subset(*slice, 0, 1), select(0, 1));
  CHECK_EQUAL(subset(*slice, 1, 1), select(1, 1));
  CHECK_EQUAL(subset(*slice, 2, 1), select(2, 1));
  CHECK_EQUAL(subset(*slice, 0, 2), select(0, 2));
  CHECK_EQUAL(subset(*slice, 1, 2), select(1, 2));
}

TEST(equality) {
  auto slice1 = make_slice();
  auto slice2 = make_slice();
  CHECK_EQUAL(*slice1, *slice2);
}

TEST(object serialization) {
  MESSAGE("make slices");
  auto slice1 = make_slice();
  auto slice2 = caf::make_counted<default_table_slice>(slice1->layout());
  MESSAGE("save content of the first slice into the buffer");
  CHECK_EQUAL(slice1->serialize(sink), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(slice2->deserialize(source), caf::none);
  MESSAGE("check result of serialization roundtrip");
  CHECK_EQUAL(*slice1, *slice2);
}

TEST(smart pointer serialization) {
  MESSAGE("make slices");
  auto slice1 = make_slice();
  table_slice_ptr slice2;
  MESSAGE("save content of the first slice into the buffer");
  CHECK_EQUAL(table_slice::serialize_ptr(sink, slice1), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(table_slice::deserialize_ptr(source, slice2), caf::none);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
}

TEST(handle serialization) {
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
}

TEST(message serialization) {
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
              caf::atom("TS_Default"));
}

TEST(rebranded message serialization) {
  MESSAGE("register factory");
  using fptr = caf::runtime_settings_map::generic_function_pointer;
  sys.runtime_settings().set(caf::atom("test"),
                             reinterpret_cast<fptr>(rebranded_slice_factory));
  MESSAGE("make rebranded slices");
  auto slice1 = caf::make_message(make_rebranded_slice());
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
              caf::atom("test"));
}

FIXTURE_SCOPE_END()
