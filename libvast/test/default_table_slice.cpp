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

#include "vast/default_table_slice.hpp"
#include "vast/event.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/view.hpp"

#define SUITE default_table_slice
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture {
  record_type layout = record_type{
    {"a", integer_type{}},
    {"b", string_type{}},
    {"c", real_type{}}
  };

  table_slice_builder_ptr builder = default_table_slice::make_builder(layout);

  using tup = std::tuple<integer, std::string, real>;

  std::vector<tup> test_data;
  std::vector<event> test_events;

  fixture() {
    REQUIRE_NOT_EQUAL(builder, nullptr);
    test_data.assign({
      tup{1, "abc", 1.2},
      tup{2, "def", 2.1},
      tup{3, "ghi", 42.},
      tup{4, "jkl", .42}
    });
    for (auto& x : test_data)
      test_events.emplace_back(event::make(make_vector(x), layout));
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

  std::vector<event> subset(size_t from, size_t num) {
    return {test_events.begin() + from, test_events.begin() + (from + num)};
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
  auto x = slice->at(0, 1);
  REQUIRE(x);
  CHECK_EQUAL(*x, make_view(foo));
  x = slice->at(1, 2);
  REQUIRE(x);
  CHECK_EQUAL(*x, make_view(4.3));
}

TEST(rows to events) {
  auto slice = make_slice();
  CHECK_EQUAL(slice->row_to_event(0), test_events[0]);
  CHECK_EQUAL(slice->row_to_event(1), test_events[1]);
  CHECK_EQUAL(slice->row_to_event(2), test_events[2]);
  CHECK_EQUAL(slice->row_to_event(3), test_events[3]);
  CHECK_EQUAL(slice->rows_to_events(), test_events);
  CHECK_EQUAL(slice->rows_to_events(0, 1), subset(0, 1));
  CHECK_EQUAL(slice->rows_to_events(1, 1), subset(1, 1));
  CHECK_EQUAL(slice->rows_to_events(2, 1), subset(2, 1));
  CHECK_EQUAL(slice->rows_to_events(0, 2), subset(0, 2));
  CHECK_EQUAL(slice->rows_to_events(1, 2), subset(1, 2));
}

FIXTURE_SCOPE_END()
