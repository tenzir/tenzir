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

#include "vast/test/fixtures/table_slices.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/test/dsl.hpp>

#include "vast/column_major_matrix_table_slice_builder.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/matrix_table_slice.hpp"
#include "vast/row_major_matrix_table_slice_builder.hpp"
#include "vast/to_events.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

class rebranded_table_slice : public default_table_slice {
public:
  static constexpr caf::atom_value class_id = caf::atom("test");

  static table_slice_ptr make(table_slice_header header) {
    return caf::make_copy_on_write<rebranded_table_slice>(std::move(header));
  }

  explicit rebranded_table_slice(table_slice_header header)
    : default_table_slice{std::move(header)} {
    // nop
  }

  caf::atom_value implementation_id() const noexcept override {
    return class_id;
  }
};

class rebranded_table_slice_builder : public default_table_slice_builder {
public:
  using super = default_table_slice_builder;

  using table_slice_type = rebranded_table_slice;

  rebranded_table_slice_builder(record_type layout) : super(std::move(layout)) {
    // Eagerly initialize to make sure super does not create slices for us.
    eager_init();
  }

  static table_slice_builder_ptr make(record_type layout) {
    return caf::make_counted<rebranded_table_slice_builder>(std::move(layout));
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
    table_slice_header header{layout(), rows(), 0};
    slice_.reset(new rebranded_table_slice{std::move(header)});
    row_ = vector(columns());
    col_ = 0;
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(table_slice_tests, fixtures::table_slices)

TEST_TABLE_SLICE(default_table_slice)
TEST_TABLE_SLICE(row_major_matrix_table_slice)
TEST_TABLE_SLICE(column_major_matrix_table_slice)
TEST_TABLE_SLICE(rebranded_table_slice)

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

TEST(select all) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto select_result = select(sut, make_ids({{100, 200}}));
  REQUIRE_EQUAL(select_result.size(), 1u);
  CHECK_EQUAL(select_result[0], sut);
}

TEST(select none) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto select_result = select(sut, make_ids({{200, 300}}));
  CHECK_EQUAL(select_result.size(), 0u);
}

TEST(select prefix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto select_result = select(sut, make_ids({{0, 150}}));
  REQUIRE_EQUAL(select_result.size(), 1u);
  CHECK_EQUAL(select_result[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*select_result[0]), to_events(*sut, 0, 50));
}

TEST(select off by one prefix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto select_result = select(sut, make_ids({{101, 151}}));
  REQUIRE_EQUAL(select_result.size(), 1u);
  CHECK_EQUAL(select_result[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*select_result[0]), to_events(*sut, 1, 50));
}

TEST(select intermediates) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto select_result = select(sut, make_ids({{110, 120}, {170, 180}}));
  REQUIRE_EQUAL(select_result.size(), 2u);
  CHECK_EQUAL(select_result[0]->rows(), 10u);
  CHECK_EQUAL(to_events(*select_result[0]), to_events(*sut, 10, 10));
  CHECK_EQUAL(select_result[1]->rows(), 10u);
  CHECK_EQUAL(to_events(*select_result[1]), to_events(*sut, 70, 10));
}

TEST(select off by one suffix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto select_result = select(sut, make_ids({{149, 199}}));
  REQUIRE_EQUAL(select_result.size(), 1u);
  CHECK_EQUAL(select_result[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*select_result[0]), to_events(*sut, 49, 50));
}

TEST(select suffix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto select_result = select(sut, make_ids({{150, 300}}));
  REQUIRE_EQUAL(select_result.size(), 1u);
  CHECK_EQUAL(select_result[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*select_result[0]), to_events(*sut, 50, 50));
}

FIXTURE_SCOPE_END()
