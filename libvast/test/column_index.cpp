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

#define SUITE column_index
#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include "vast/column_index.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    directory /= "column-index";
  }

  auto lookup(column_index_ptr& idx, const curried_predicate& pred) {
    return unbox(idx->lookup(pred.op, make_view(pred.rhs)));
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(column_index_tests, fixture)

TEST(skip attribute) {
  auto foo_type = integer_type{}.name("foo");
  auto bar_type = integer_type{}.attributes({{"skip"}}).name("bar");
  auto foo = unbox(make_column_index(sys, directory, foo_type, 0));
  auto bar = unbox(make_column_index(sys, directory, bar_type, 1));
  CHECK_EQUAL(foo->has_skip_attribute(), false);
  CHECK_EQUAL(bar->has_skip_attribute(), true);
}

TEST(integer values) {
  MESSAGE("ingest integer values");
  integer_type column_type;
  record_type layout{{"value", column_type}};
  auto col = unbox(make_column_index(sys, directory, column_type, 0));
  auto rows = make_rows(1, 2, 3, 1, 2, 3, 1, 2, 3);
  auto slice = default_table_slice::make(layout, rows);
  col->add(slice);
  REQUIRE_EQUAL(slice->rows(), rows.size());
  auto slice_size = rows.size();
  MESSAGE("generate test queries");
  auto is1 = curried(unbox(to<predicate>(":int == +1")));
  auto is2 = curried(unbox(to<predicate>(":int == +2")));
  auto is3 = curried(unbox(to<predicate>(":int == +3")));
  auto is4 = curried(unbox(to<predicate>(":int == +4")));
  MESSAGE("verify column index");
  CHECK_EQUAL(lookup(col, is1), make_ids({0, 3, 6}, slice_size));
  CHECK_EQUAL(lookup(col, is2), make_ids({1, 4, 7}, slice_size));
  CHECK_EQUAL(lookup(col, is3), make_ids({2, 5, 8}, slice_size));
  CHECK_EQUAL(lookup(col, is4), make_ids({}, slice_size));
  MESSAGE("persist and reload from disk");
  col->flush_to_disk();
  col.reset();
  col = unbox(make_column_index(sys, directory, column_type, 0));
  MESSAGE("verify column index again");
  CHECK_EQUAL(lookup(col, is1), make_ids({0, 3, 6}, slice_size));
  CHECK_EQUAL(lookup(col, is2), make_ids({1, 4, 7}, slice_size));
  CHECK_EQUAL(lookup(col, is3), make_ids({2, 5, 8}, slice_size));
  CHECK_EQUAL(lookup(col, is4), make_ids({}, slice_size));
}

TEST(bro conn log) {
  MESSAGE("ingest originators from bro conn log");
  auto row_type = bro_conn_log_layout();
  auto col_offset = unbox(row_type.resolve("id.orig_h"));
  auto col_type = row_type.at(col_offset);
  auto col_index = unbox(row_type.flat_index_at(col_offset));
  REQUIRE_EQUAL(col_index, 2u); // 3rd column
  auto col = unbox(make_column_index(sys, directory, *col_type, col_index));
  for (auto slice : bro_conn_log_slices)
    col->add(slice);
  MESSAGE("verify column index");
  auto pred = curried(unbox(to<predicate>(":addr == 192.168.1.103")));
  auto expected_result = make_ids({1, 3, 7, 14, 16}, bro_conn_log.size());
  CHECK_EQUAL(lookup(col, pred), expected_result);
  MESSAGE("persist and reload from disk");
  col->flush_to_disk();
  col.reset();
  MESSAGE("verify column index again");
  col = unbox(make_column_index(sys, directory, *col_type, col_index));
  CHECK_EQUAL(lookup(col, pred), expected_result);
}

FIXTURE_SCOPE_END()
