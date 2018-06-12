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
#include "test.hpp"

#include "fixtures/events.hpp"
#include "fixtures/filesystem.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/system/column_index.hpp"
#include "vast/type.hpp"

using namespace vast;
using namespace vast::system;

namespace {

struct fixture : fixtures::events, fixtures::filesystem {
  fixture() {
    directory /= "column-index";
  }

  template <class T>
  T unbox(expected<T> x) {
    REQUIRE(x);
    return std::move(*x);
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(column_index_tests, fixture)

TEST(flat column type) {
  MESSAGE("ingest integer values");
  integer_type column_type;
  auto col = unbox(make_flat_data_index(directory, column_type));
  std::vector<int> xs{1, 2, 3, 1, 2, 3, 1, 2, 3};
  size_t next_id = 0;
  for (auto i : xs)
    col->add(event::make(i, column_type, next_id++));
  MESSAGE("generate test queries");
  auto is1 = unbox(to<predicate>(":int == +1"));
  auto is2 = unbox(to<predicate>(":int == +2"));
  auto is3 = unbox(to<predicate>(":int == +3"));
  auto is4 = unbox(to<predicate>(":int == +4"));
  MESSAGE("verify column index");
  CHECK_EQUAL(unbox(col->lookup(is1)), make_ids({0, 3, 6}, xs.size()));
  CHECK_EQUAL(unbox(col->lookup(is2)), make_ids({1, 4, 7}, xs.size()));
  CHECK_EQUAL(unbox(col->lookup(is3)), make_ids({2, 5, 8}, xs.size()));
  CHECK_EQUAL(unbox(col->lookup(is4)), make_ids({}, xs.size()));
  MESSAGE("persist and reload from disk");
  col->flush_to_disk();
  col.reset();
  col = unbox(make_flat_data_index(directory, integer_type{}));
  MESSAGE("verify column index again");
  CHECK_EQUAL(unbox(col->lookup(is1)), make_ids({0, 3, 6}, xs.size()));
  CHECK_EQUAL(unbox(col->lookup(is2)), make_ids({1, 4, 7}, xs.size()));
  CHECK_EQUAL(unbox(col->lookup(is3)), make_ids({2, 5, 8}, xs.size()));
  CHECK_EQUAL(unbox(col->lookup(is4)), make_ids({}, xs.size()));
}

TEST(bro conn log) {
  MESSAGE("ingest origins from bro conn log");
  auto row_type = get<record_type>(bro_conn_log[0].type());
  auto col_offset = unbox(row_type.resolve(key{"id", "orig_h"}));
  auto col_type = row_type.at(col_offset);
  auto col = unbox(make_field_data_index(directory, *col_type, col_offset));
  size_t next_id = 0;
  for (auto entry : bro_conn_log) {
    entry.id(next_id++);
    col->add(std::move(entry));
  }
  MESSAGE("verify column index");
  auto pred = unbox(to<predicate>(":addr == 169.254.225.22"));
  auto expected_result = make_ids({680, 682, 719, 720}, bro_conn_log.size());
  CHECK_EQUAL(unbox(col->lookup(pred)), expected_result );
  MESSAGE("persist and reload from disk");
  col->flush_to_disk();
  col.reset();
  MESSAGE("verify column index again");
  col = unbox(make_field_data_index(directory, *col_type, col_offset));
}

FIXTURE_SCOPE_END()
