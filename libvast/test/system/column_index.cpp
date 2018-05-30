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

TEST(flat type) {
  MESSAGE("ingest integer values");
  auto col = unbox(make_flat_data_index(directory, integer_type{}));
  std::vector<int> xs{1, 2, 3, 1, 2, 3, 1, 2, 3};
  for (size_t i = 0; i < xs.size(); ++i) {
    event x{xs[i]};
    x.id(i);
    col->add(std::move(x));
  }
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

FIXTURE_SCOPE_END()
