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

#include "vast/ids.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

namespace {

#if 0

/// Constructs table slices filled with random content for testing purposes.
/// @param num_slices The number of table slices to generate.
/// @param slice_size The number of rows per table slices.
/// @param layout The layout of the table slice.
/// @param offset The offset of the first table slize.
/// @param seed The seed value for initializing the random-number generator.
/// @returns a list of randomnly filled table slices or an error.
/// @relates table_slice
caf::expected<std::vector<table_slice>>
make_random_table_slices(std::vector<table_slice>::size_type num_slices,
                         table_slice::size_type slice_size,
                         record_type layout, id offset = 0, size_t seed = 0) {
  auto test_schema = schema{};
  test_schema.add(layout);
  // We have no access to the actor system, so we can only pick the default
  // table slice type here. This ignores any user-defined overrides.
  // However, this function is only meant for testing anyways.
  auto opts = caf::settings{};
  caf::put(opts, "vast.import.test.seed", seed);
  caf::put(opts, "vast.import.max-events", std::numeric_limits<size_t>::max());
  format::test::reader generator{defaults::import::table_slice_type,
                                 std::move(opts), nullptr};
  generator.schema(std::move(test_schema));
  auto result = std::vector<table_slice>{};
  auto add_slice = [&](table_slice&& slice) {
    slice.offset(offset);
    offset += slice.rows();
    result.emplace_back(std::move(slice));
  };
  result.reserve(num_slices);
  if (auto err
      = generator.read(num_slices * slice_size, slice_size, add_slice).first)
    return err;
  return result;
}

/// Converts the table slice into a 2-D matrix in row-major order such that
/// each row represents an event.
/// @param slice The table slice to convert.
/// @param first_row An offset to the first row to consider.
/// @param num_rows Then number of rows to consider. (0 = all rows)
/// @returns a 2-D matrix of data instances corresponding to *slice*.
/// @pre `first_row < slice.num_rows()`
/// @pre `num_rows <= slice.num_rows() - first_row`
/// @note This function exists primarily for unit testing because it performs
/// excessive memory allocations.
/// @relates table_slice
std::vector<std::vector<data>>
to_data(const table_slice& slice, v1::table_slice::size_type first_row = 0,
        table_slice::size_type num_rows = 0) {
  VAST_ASSERT(first_row < slice.num_rows());
  VAST_ASSERT(num_rows <= slice.num_rows() - first_row);
  if (num_rows == 0)
    num_rows = slice.num_rows() - first_row;
  std::vector<std::vector<data>> result;
  result.reserve(num_rows);
  for (size_t i = 0; i < num_rows; ++i) {
    std::vector<data> xs;
    xs.reserve(slice.num_columns());
    for (size_t j = 0; j < slice.num_columns(); ++j)
      xs.emplace_back(materialize(slice.at(first_row + i, j)));
    result.push_back(std::move(xs));
  }
  return result;
}

/// Converts the table slices into a 2-D matrix in row-major order such that
/// each row represents an event.
/// @param slices The table slices to convert.
/// @note This function exists primarily for unit testing because it performs
/// excessive memory allocations.
/// @relates table_slice
std::vector<std::vector<data>>
to_data(const std::vector<table_slice>& slices) {
  std::vector<std::vector<data>> result;
  result.reserve(num_rows(slices));
  for (auto& slice : slices)
    detail::append(result, to_data(slice));
  return result;
}

#endif

} // namespace

FIXTURE_SCOPE(table_slice_tests, fixtures::table_slices)

TEST(random integer slices) {
  record_type layout{{"i", integer_type{}.attributes({{"default", "uniform(100,"
                                                                  "200)"}})}};
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
  auto sut = zeek_conn_log[0];
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
  auto sut = zeek_conn_log[0];
  for (size_t row = 0; row < sut->rows(); ++row) {
    auto rview = sut->row(row);
    CHECK_EQUAL(rview.row(), row);
    CHECK_EQUAL(rview.columns(), sut->columns());
    for (size_t column = 0; column < rview.columns(); ++column)
      CHECK_EQUAL(rview[column], sut->at(row, column));
  }
}

TEST(select all) {
  auto sut = zeek_conn_log_full[0];
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{100, 200}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0], sut);
}

TEST(select none) {
  auto sut = zeek_conn_log_full[0];
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{200, 300}}));
  CHECK_EQUAL(xs.size(), 0u);
}

TEST(select prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{0, 150}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_data(*xs[0]), to_data(*sut, 0, 50));
}

TEST(select off by one prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{101, 151}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_data(*xs[0]), to_data(*sut, 1, 50));
}

TEST(select intermediates) {
  auto sut = zeek_conn_log_full[0];
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{110, 120}, {170, 180}}));
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs[0]->rows(), 10u);
  CHECK_EQUAL(to_data(*xs[0]), to_data(*sut, 10, 10));
  CHECK_EQUAL(xs[1]->rows(), 10u);
  CHECK_EQUAL(to_data(*xs[1]), to_data(*sut, 70, 10));
}

TEST(select off by one suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{149, 199}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_data(*xs[0]), to_data(*sut, 49, 50));
}

TEST(select suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.unshared().offset(100);
  auto xs = select(sut, make_ids({{150, 300}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0]->rows(), 50u);
  CHECK_EQUAL(to_data(*xs[0]), to_data(*sut, 50, 50));
}

TEST(truncate) {
  auto sut = zeek_conn_log[0];
  REQUIRE_EQUAL(sut->rows(), 8u);
  sut.unshared().offset(100);
  auto truncated_events = [&](size_t num_rows) {
    auto sub_slice = truncate(sut, num_rows);
    if (sub_slice->rows() != num_rows)
      FAIL("expected " << num_rows << " rows, got " << sub_slice->rows());
    return to_data(*sub_slice);
  };
  auto sub_slice = truncate(sut, 8);
  CHECK_EQUAL(*sub_slice, *sut);
  CHECK_EQUAL(truncated_events(7), to_data(*sut, 0, 7));
  CHECK_EQUAL(truncated_events(6), to_data(*sut, 0, 6));
  CHECK_EQUAL(truncated_events(5), to_data(*sut, 0, 5));
  CHECK_EQUAL(truncated_events(4), to_data(*sut, 0, 4));
  CHECK_EQUAL(truncated_events(3), to_data(*sut, 0, 3));
  CHECK_EQUAL(truncated_events(2), to_data(*sut, 0, 2));
  CHECK_EQUAL(truncated_events(1), to_data(*sut, 0, 1));
}

TEST(split) {
  auto sut = zeek_conn_log[0];
  REQUIRE_EQUAL(sut->rows(), 8u);
  sut.unshared().offset(100);
  // Splits `sut` using to_data.
  auto manual_split_sut = [&](size_t parition_point) {
    return std::pair{to_data(*sut, 0, parition_point),
                     to_data(*sut, parition_point)};
  };
  // Splits `sut` using split() and then converting to events.
  auto split_sut = [&](size_t parition_point) {
    auto [first, second] = split(sut, parition_point);
    if (first->rows() + second->rows() != 8)
      FAIL("expected 8 rows in total, got "
           << (first->rows() + second->rows()));
    return std::pair{to_data(*first), to_data(*second)};
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

FIXTURE_SCOPE_END()
