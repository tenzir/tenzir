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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/table_slice_row.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(table_slice_tests, fixtures::table_slices)

TEST(random integer slices) {
  record_type layout{
    {"i", integer_type{}.attributes({{"default", "uniform(100,200)"}})}};
  layout.name("test.integers");
  auto slices = unbox(make_random_table_slices(10, 10, layout));
  CHECK_EQUAL(slices.size(), 10u);
  CHECK(std::all_of(slices.begin(), slices.end(),
                    [](auto& slice) { return slice.rows() == 10; }));
  std::vector<integer> values;
  for (auto& slice : slices)
    for (size_t row = 0; row < slice.rows(); ++row)
      values.emplace_back(get<integer>(slice.at(row, 0)));
  auto [lowest, highest] = std::minmax_element(values.begin(), values.end());
  CHECK_GREATER_EQUAL(*lowest, 100);
  CHECK_LESS_EQUAL(*highest, 200);
}

TEST(column view) {
  auto sut = zeek_conn_log[0];
  auto ts_cview = table_slice_column::make(sut, "ts");
  REQUIRE(ts_cview);
  CHECK_EQUAL(ts_cview->index(), 0u);
  for (size_t column = 0; column < sut.columns(); ++column) {
    auto cview = table_slice_column{sut, column};
    REQUIRE_NOT_EQUAL(cview.size(), 0u);
    CHECK_EQUAL(cview.index(), column);
    CHECK_EQUAL(cview.size(), sut.rows());
    for (size_t row = 0; row < cview.size(); ++row)
      CHECK_EQUAL(cview[row], sut.at(row, column));
  }
}

TEST(row view) {
  auto sut = zeek_conn_log[0];
  for (size_t row = 0; row < sut.rows(); ++row) {
    auto rview = table_slice_row{sut, row};
    REQUIRE_NOT_EQUAL(rview.size(), 0u);
    CHECK_EQUAL(rview.index(), row);
    CHECK_EQUAL(rview.size(), sut.columns());
    for (size_t column = 0; column < rview.size(); ++column)
      CHECK_EQUAL(rview[column], sut.at(row, column));
  }
}

TEST(select all) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{100, 200}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0], sut);
}

TEST(select none) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{200, 300}}));
  CHECK_EQUAL(xs.size(), 0u);
}

TEST(select prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{0, 150}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(to_data(xs[0]), to_data(sut, 0, 50));
}

TEST(select off by one prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{101, 151}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(to_data(xs[0]), to_data(sut, 1, 50));
}

TEST(select intermediates) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{110, 120}, {170, 180}}));
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs[0].rows(), 10u);
  CHECK_EQUAL(to_data(xs[0]), to_data(sut, 10, 10));
  CHECK_EQUAL(xs[1].rows(), 10u);
  CHECK_EQUAL(to_data(xs[1]), to_data(sut, 70, 10));
}

TEST(select off by one suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{149, 199}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(to_data(xs[0]), to_data(sut, 49, 50));
}

TEST(select suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{150, 300}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(to_data(xs[0]), to_data(sut, 50, 50));
}

TEST(truncate) {
  auto sut = zeek_conn_log[0];
  REQUIRE_EQUAL(sut.rows(), 8u);
  sut.offset(100);
  auto truncated_events = [&](size_t num_rows) {
    auto sub_slice = truncate(sut, num_rows);
    if (sub_slice.rows() != num_rows)
      FAIL("expected " << num_rows << " rows, got " << sub_slice.rows());
    return to_data(sub_slice);
  };
  auto sub_slice = truncate(sut, 8);
  CHECK_EQUAL(sub_slice, sut);
  CHECK_EQUAL(truncated_events(7), to_data(sut, 0, 7));
  CHECK_EQUAL(truncated_events(6), to_data(sut, 0, 6));
  CHECK_EQUAL(truncated_events(5), to_data(sut, 0, 5));
  CHECK_EQUAL(truncated_events(4), to_data(sut, 0, 4));
  CHECK_EQUAL(truncated_events(3), to_data(sut, 0, 3));
  CHECK_EQUAL(truncated_events(2), to_data(sut, 0, 2));
  CHECK_EQUAL(truncated_events(1), to_data(sut, 0, 1));
}

TEST(split) {
  auto sut = zeek_conn_log[0];
  REQUIRE_EQUAL(sut.rows(), 8u);
  sut.offset(100);
  // Splits `sut` using to_data.
  auto manual_split_sut = [&](size_t parition_point) {
    return std::pair{to_data(sut, 0, parition_point),
                     to_data(sut, parition_point)};
  };
  // Splits `sut` using split() and then converting to events.
  auto split_sut = [&](size_t parition_point) {
    auto [first, second] = split(sut, parition_point);
    if (first.rows() + second.rows() != 8)
      FAIL("expected 8 rows in total, got " << (first.rows() + second.rows()));
    return std::pair{to_data(first), to_data(second)};
  };
  // We compare the results of the two lambdas, meaning that it should make no
  // difference whether we split via `to_data` or `split`.
  CHECK_EQUAL(split_sut(1), manual_split_sut(1));
  CHECK_EQUAL(split_sut(2), manual_split_sut(2));
  CHECK_EQUAL(split_sut(3), manual_split_sut(3));
  CHECK_EQUAL(split_sut(4), manual_split_sut(4));
  CHECK_EQUAL(split_sut(5), manual_split_sut(5));
  CHECK_EQUAL(split_sut(6), manual_split_sut(6));
  CHECK_EQUAL(split_sut(7), manual_split_sut(7));
}

TEST(evaluate) {
  auto sut = zeek_conn_log[0];
  sut.offset(0);
  auto check_eval
    = [&](std::string_view expr, std::initializer_list<id_range> id_init) {
        auto ids = make_ids(std::move(id_init), sut.offset() + sut.rows());
        auto exp = unbox(to<expression>(std::move(expr)));
        CHECK_EQUAL(evaluate(exp, sut), ids);
      };
  check_eval("#type == \"zeek.conn\"", {{0, 8}});
  check_eval("#type != \"zeek.conn\"", {});
  check_eval("#field == \"orig_pkts\"", {{0, 8}});
  check_eval("#field != \"orig_pkts\"", {});
}

FIXTURE_SCOPE_END()
