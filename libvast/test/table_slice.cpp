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

#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/test/dsl.hpp>

#include "vast/default_table_slice.hpp"
#include "vast/default_table_slice_builder.hpp"
#include "vast/ids.hpp"
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
TEST_TABLE_SLICE(rebranded_table_slice)

TEST(random integer slices) {
  record_type layout{
    {"i", integer_type{}.attributes({{"default", "uniform(100,200)"}})}};
  layout.name("test.integers");
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

TEST(column view) {
  auto sut = zeek_conn_log_slices[0];
  CHECK_EQUAL(unbox(sut->column("ts")).column(), 0u);
  for (size_t column = 0; column < sut->columns(); ++column) {
    auto cview = sut->column(column);
    CHECK_EQUAL(cview.column(), column);
    CHECK_EQUAL(cview.rows(), sut->rows());
    for (size_t row = 0; row < cview.rows(); ++row)
      CHECK_EQUAL(cview[row], sut->at(row, column));
  }
}

TEST(row view) {
  auto sut = zeek_conn_log_slices[0];
  for (size_t row = 0; row < sut->rows(); ++row) {
    auto rview = sut->row(row);
    CHECK_EQUAL(rview.row(), row);
    CHECK_EQUAL(rview.columns(), sut->columns());
    for (size_t column = 0; column < rview.columns(); ++column)
      CHECK_EQUAL(rview[column], sut->at(row, column));
  }
}

TEST(select all) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{100, 200}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0], sut);
}

TEST(select none) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{200, 300}}));
  CHECK_EQUAL(xs.size(), 0u);
}

TEST(select prefix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{0, 150}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*xs[0]), to_events(*sut, 0, 50));
}

TEST(select off by one prefix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{101, 151}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*xs[0]), to_events(*sut, 1, 50));
}

TEST(select intermediates) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{110, 120}, {170, 180}}));
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs[0]->rows(), 10u);
  CHECK_EQUAL(to_events(*xs[0]), to_events(*sut, 10, 10));
  CHECK_EQUAL(xs[1]->rows(), 10u);
  CHECK_EQUAL(to_events(*xs[1]), to_events(*sut, 70, 10));
}

TEST(select off by one suffix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{149, 199}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*xs[0]), to_events(*sut, 49, 50));
}

TEST(select suffix) {
  auto sut = zeek_full_conn_log_slices.front();
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{150, 300}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_events(*xs[0]), to_events(*sut, 50, 50));
}

TEST(truncate) {
  auto sut = zeek_conn_log_slices.front();
  REQUIRE_EQUAL(sut->rows(), 8u);
  sut.unshared().offset(100);
  auto truncated_events = [&](size_t num_rows) {
    auto sub_slice = truncate(sut, num_rows);
    if (sub_slice->rows() != num_rows)
      FAIL("expected " << num_rows << " rows, got " << sub_slice->rows());
    return to_events(*sub_slice);
  };
  auto sub_slice = truncate(sut, 8);
  CHECK_EQUAL(*sub_slice, *sut);
  CHECK_EQUAL(truncated_events(7), to_events(*sut, 0, 7));
  CHECK_EQUAL(truncated_events(6), to_events(*sut, 0, 6));
  CHECK_EQUAL(truncated_events(5), to_events(*sut, 0, 5));
  CHECK_EQUAL(truncated_events(4), to_events(*sut, 0, 4));
  CHECK_EQUAL(truncated_events(3), to_events(*sut, 0, 3));
  CHECK_EQUAL(truncated_events(2), to_events(*sut, 0, 2));
  CHECK_EQUAL(truncated_events(1), to_events(*sut, 0, 1));
}

TEST(split) {
  auto sut = zeek_conn_log_slices.front();
  REQUIRE_EQUAL(sut->rows(), 8u);
  sut.unshared().offset(100);
  // Splits `sut` using to_events.
  auto manual_split_sut = [&](size_t parition_point) {
    return std::pair{to_events(*sut, 0, parition_point),
                     to_events(*sut, parition_point)};
  };
  // Splits `sut` using split() and then converting to events.
  auto split_sut = [&](size_t parition_point) {
    // auto [first, second] = split(sut, parition_point);
    // if (first->rows() + second->rows() != 8)
    //  FAIL("expected 8 rows in total, got "
    //       << (first->rows() + second->rows()));
    // return std::pair{to_events(*first), to_events(*second)};
    auto pair = split(sut, parition_point);
    if (pair.first->rows() + pair.second->rows() != 8)
      FAIL("expected 8 rows in total, got "
           << (pair.first->rows() + pair.second->rows()));
    return std::pair{to_events(*pair.first), to_events(*pair.second)};
  };
  // We compare the results of the two lambdas, meaning that it should make no
  // difference whether we split via `to_events` or `split`.
  CHECK_EQUAL(split_sut(1), manual_split_sut(1));
  CHECK_EQUAL(split_sut(2), manual_split_sut(2));
  CHECK_EQUAL(split_sut(3), manual_split_sut(3));
  CHECK_EQUAL(split_sut(4), manual_split_sut(4));
  CHECK_EQUAL(split_sut(5), manual_split_sut(5));
  CHECK_EQUAL(split_sut(6), manual_split_sut(6));
  CHECK_EQUAL(split_sut(7), manual_split_sut(7));
}

FIXTURE_SCOPE_END()
