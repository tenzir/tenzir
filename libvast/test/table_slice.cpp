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

#define SUITE table_slice

#include "vast/table_slice.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/test/dsl.hpp>

#include "vast/column_major_matrix_table_slice_builder.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/row_major_matrix_table_slice_builder.hpp"
#include "vast/value.hpp"
#include "vast/value_index.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

class rebranded_table_slice : public default_table_slice {
public:
  static constexpr caf::atom_value class_id = caf::atom("TS_Test");

  using super = default_table_slice;

  using super::super;

  rebranded_table_slice(const default_table_slice& other) : super(other) {
    // nop
  }

  caf::atom_value implementation_id() const noexcept override {
    return class_id;
  }
};

class rebranded_table_slice_builder : public default_table_slice_builder {
public:
  using super = default_table_slice_builder;

  rebranded_table_slice_builder(record_type layout) : super(std::move(layout)) {
    // Eagerly initialize to make sure super does not create slices for us.
    eager_init();
  }

  static table_slice_builder_ptr make(record_type layout) {
    return caf::make_counted<rebranded_table_slice_builder>(std::move(layout));
  }

  static table_slice_ptr make_slice(record_type layout,
                                    table_slice::size_type) {
    return caf::make_copy_on_write<rebranded_table_slice>(std::move(layout));
  }

  table_slice_ptr finish() override {
    auto result = super::finish();
    eager_init();
    return result;
  }

  caf::atom_value implementation_id() const noexcept override {
    return get_implementation_id();
  }

  static caf::atom_value get_implementation_id() noexcept {
    return rebranded_table_slice::class_id;
  }

private:
  void eager_init() {
    slice_.reset(new rebranded_table_slice(this->layout()));
    row_ = vector(layout().fields.size());
    col_ = 0;
  }
};

struct fixture : fixtures::deterministic_actor_system {
  record_type layout = record_type{
    {"a", integer_type{}},
    {"b", string_type{}},
    {"c", real_type{}}
  };

  std::vector<table_slice_builder_ptr> builders{
    default_table_slice_builder::make(layout),
    rebranded_table_slice_builder::make(layout),
    row_major_matrix_table_slice_builder::make(layout),
    column_major_matrix_table_slice_builder::make(layout),
  };

  using tup = std::tuple<integer, std::string, real>;

  std::vector<tup> test_data;

  std::vector<value> test_values;

  std::vector<char> buf;

  caf::binary_serializer sink;

  auto make_source() {
    return caf::binary_deserializer{sys, buf};
  }

  template <class Builder>
  void add_slice_factory() {
    using fptr = caf::runtime_settings_map::generic_function_pointer;
    sys.runtime_settings().set(Builder::get_implementation_id(),
                               reinterpret_cast<fptr>(Builder::make_slice));
  }

  fixture() : sink(sys, buf) {
    if (std::any_of(builders.begin(), builders.end(),
                    [](auto& ptr) { return ptr == nullptr; }))
      FAIL("one of the table slice builder factories returned nullptr");
    // Initialize state.
    test_data.assign({
      tup{1, "abc", 1.2},
      tup{2, "def", 2.1},
      tup{3, "ghi", 42.},
      tup{4, "jkl", .42}
    });
    for (auto& x : test_data)
      test_values.emplace_back(value::make(make_vector(x), layout));
    // Register factory.
    add_slice_factory<rebranded_table_slice_builder>();
    add_slice_factory<row_major_matrix_table_slice_builder>();
    add_slice_factory<column_major_matrix_table_slice_builder>();
  }

  auto make_slice(table_slice_builder& builder) {
    for (auto& x : test_data)
      std::apply(
        [&](auto... xs) {
          if ((!builder.add(make_view(xs)) || ...))
            FAIL("builder failed to add element");
        },
        x);
    return builder.finish();
  }

  std::vector<value> select(size_t from, size_t num) {
    return {test_values.begin() + from, test_values.begin() + (from + num)};
  }

  void test_add(table_slice_builder& builder) {
    MESSAGE(">> test table_slice_builder::add");
    MESSAGE("1st row");
    auto foo = "foo"s;
    auto bar = "foo"s;
    CHECK(builder.add(make_view(42)));
    CHECK(!builder.add(make_view(true))); // wrong type
    CHECK(builder.add(make_view(foo)));
    CHECK(builder.add(make_view(4.2)));
    MESSAGE("2nd row");
    CHECK(builder.add(make_view(43)));
    CHECK(builder.add(make_view(bar)));
    CHECK(builder.add(make_view(4.3)));
    MESSAGE("finish");
    auto slice = builder.finish();
    CHECK_EQUAL(slice->rows(), 2u);
    CHECK_EQUAL(slice->columns(), 3u);
    CHECK_EQUAL(slice->at(0, 1), make_view(foo));
    CHECK_EQUAL(slice->at(1, 2), make_view(4.3));
  }

  void test_equality(table_slice_builder& builder) {
    MESSAGE(">> test equality");
    auto slice1 = make_slice(builder);
    auto slice2 = make_slice(builder);
    CHECK_EQUAL(*slice1, *slice2);
  }

  void test_copy(table_slice_builder& builder) {
    MESSAGE(">> test copy");
    auto slice1 = make_slice(builder);
    table_slice_ptr slice2{slice1->copy(), false};
    CHECK_EQUAL(*slice1, *slice2);
  }

  void test_manual_serialization(table_slice_builder& builder) {
    MESSAGE(">> test manual serialization via serialize_ptr and deserialize_ptr");
    MESSAGE("make slices");
    auto slice1 = make_slice(builder);
    table_slice_ptr slice2;
    MESSAGE("save content of the first slice into the buffer");
    CHECK_EQUAL(table_slice::serialize_ptr(sink, slice1), caf::none);
    MESSAGE("load content for the second slice from the buffer");
    auto source = make_source();
    CHECK_EQUAL(table_slice::deserialize_ptr(source, slice2), caf::none);
    MESSAGE("check result of serialization roundtrip");
    REQUIRE_NOT_EQUAL(slice2, nullptr);
    CHECK_EQUAL(*slice1, *slice2);
    buf.clear();
  }

  void test_smart_pointer_serialization(table_slice_builder& builder) {
    MESSAGE(">> test smart pointer serialization");
    MESSAGE("make slices");
    auto slice1 = make_slice(builder);
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

  void test_message_serialization(table_slice_builder& builder) {
    MESSAGE(">> test message serialization");
    MESSAGE("make slices");
    auto slice1 = caf::make_message(make_slice(builder));
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
                builder.implementation_id());
    buf.clear();
  }

  void test_apply_column(table_slice_builder& builder) {
    MESSAGE(">> test apply_column");
    auto idx = value_index::make(integer_type{});
    REQUIRE(idx != nullptr);
    auto slice = make_slice(builder);
    slice->apply_column(0, *idx);
    CHECK_EQUAL(idx->offset(), 4u);
    constexpr auto less = relational_operator::less;
    CHECK_EQUAL(unbox(idx->lookup(less, vast::make_view(3))),
                make_ids({0, 1}, 4));
  }

  void test_implementations() {
    for (auto& builder : builders) {
      MESSAGE("> test implementation " << builder->implementation_id());
      test_add(*builder);
      test_equality(*builder);
      test_copy(*builder);
      test_manual_serialization(*builder);
      test_smart_pointer_serialization(*builder);
      test_message_serialization(*builder);
      test_apply_column(*builder);
    }
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(table_slice_tests, fixture)

TEST(implementations) {
  test_implementations();
}

TEST(random integer slices) {
  record_type layout{
    {"i", integer_type{}.attributes({{"default", "uniform(100,200)"}})}};
  layout.name("test");
  auto slices = unbox(make_random_table_slices(10, 10, layout));
  CHECK_EQUAL(slices.size(), 10u);
  CHECK(std::all_of(slices.begin(), slices.end(),
                    [](auto& slice) { return slice->rows() == 10; }));
  std::vector<integer> values;
  for (auto& slice : slices)
    for (size_t row = 0; row < slice->rows(); ++row)
      values.emplace_back(get<integer>(slice->at(row, 0)));
  auto [lowest, highest] = std::minmax_element(values.begin(), values.end());
  CHECK_GREATER_EQUAL(*lowest, 100);
  CHECK_LESS_EQUAL(*highest, 200);
}

FIXTURE_SCOPE_END()
