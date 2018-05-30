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

#define SUITE table_index
#include "test.hpp"

#include "fixtures/events.hpp"
#include "fixtures/filesystem.hpp"

#include "vast/bitmap.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/system/table_index.hpp"

using namespace vast;
using namespace vast::system;

namespace {

struct fixture : fixtures::events, fixtures::filesystem {
  fixture() {
    directory /= "column-layout";
  }

  template <class T>
  T unbox(expected<T> x) {
    REQUIRE(x);
    return std::move(*x);
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(table_index_tests, fixture)

TEST(flat type) {
  // Some test data.
  std::vector<int> xs{1, 2, 3, 1, 2, 3, 1, 2, 3};
  MESSAGE("generate test queries");
  auto is1 = unbox(to<predicate>(":int == +1"));
  auto is2 = unbox(to<predicate>(":int == +2"));
  auto is3 = unbox(to<predicate>(":int == +3"));
  auto is4 = unbox(to<predicate>(":int == +4"));
  { // lifetime of the first table index
    auto tbl = unbox(make_table_index(directory, integer_type{}));
    MESSAGE("ingest integer values");
    for (size_t i = 0; i < xs.size(); ++i) {
      event x{xs[i]};
      x.id(i);
      tbl.add(std::move(x));
    }
    MESSAGE("verify table index");
    CHECK_EQUAL(unbox(tbl.lookup(is1)), make_ids({0, 3, 6}, xs.size()));
    CHECK_EQUAL(unbox(tbl.lookup(is2)), make_ids({1, 4, 7}, xs.size()));
    CHECK_EQUAL(unbox(tbl.lookup(is3)), make_ids({2, 5, 8}, xs.size()));
    CHECK_EQUAL(unbox(tbl.lookup(is4)), make_ids({}, xs.size()));
    MESSAGE("(automatically) persist table index to disk");
  }
  { // lifetime scope of the second table index
    MESSAGE("restore table index from disk");
    auto tbl = unbox(make_table_index(directory, integer_type{}));
    MESSAGE("verify table index again");
    CHECK_EQUAL(unbox(tbl.lookup(is1)), make_ids({0, 3, 6}, xs.size()));
    CHECK_EQUAL(unbox(tbl.lookup(is2)), make_ids({1, 4, 7}, xs.size()));
    CHECK_EQUAL(unbox(tbl.lookup(is3)), make_ids({2, 5, 8}, xs.size()));
    CHECK_EQUAL(unbox(tbl.lookup(is4)), make_ids({}, xs.size()));
  }
}

TEST(record type) {
  auto tbl_type = record_type{
    {"x", record_type{
      {"a", integer_type{}},
      {"b", boolean_type{}}
    }},
    {"y", record_type{
      {"a", string_type{}}
    }}
  };
  auto mk_row = [&](int x, bool y, std::string z) {
    return value::make(vector{vector{x, y}, vector{std::move(z)}}, tbl_type);
  };
  // Some test data.
  std::vector<value> xs{mk_row(1, true, "abc"),    mk_row(10, false, "def"),
                        mk_row(5, true, "hello"),  mk_row(1, true, "d e f"),
                        mk_row(15, true, "world"), mk_row(5, true, "bar"),
                        mk_row(10, true, "a b c"), mk_row(10, true, "baz"),
                        mk_row(5, true, "foo"),    mk_row(1, true, "test")};
  MESSAGE(caf::deep_to_string(xs));
  MESSAGE("generate test queries");
  auto x_a_is1 = unbox(to<predicate>("x.a == +1"));
  { // lifetime of the first table index
    auto tbl = unbox(make_table_index(directory, tbl_type));
    CHECK_EQUAL(tbl.num_data_columns(), 3u);
    MESSAGE("ingest table values");
    for (size_t i = 0; i < xs.size(); ++i) {
      event x{xs[i]};
      x.id(i);
      MESSAGE("ingest" <<  caf::deep_to_string(x));
      tbl.add(std::move(x));
    }
    MESSAGE("verify table index");
    CHECK_EQUAL(unbox(tbl.lookup(x_a_is1)), make_ids({0, 3, 9}, xs.size()));
    MESSAGE("(automatically) persist table index to disk");
  }
  { // lifetime scope of the second table index
    MESSAGE("restore table index from disk");
    auto tbl = unbox(make_table_index(directory, tbl_type));
    MESSAGE("verify table index again");
    CHECK_EQUAL(unbox(tbl.lookup(x_a_is1)), make_ids({0, 3, 9}, xs.size()));
  }
}

TEST(bro conn logs) {
  auto pred = unbox(to<predicate>("id.resp_p == 995/?"));
  const auto tbl_type = bro_conn_log[0].type();
  { // lifetime of the first table index
    MESSAGE("generate column layout for bro conn logs");
    auto tbl = unbox(make_table_index(directory, tbl_type));
    CHECK_EQUAL(tbl.num_meta_columns(), 2u);
    MESSAGE("ingesting events");
    for (auto& entry : bro_conn_log) {
      auto err = tbl.add(entry);
      if (err) {
        FAIL("error during ingestion: " << caf::to_string(err));
      }
    }
    MESSAGE("verify table index");
    auto result = unbox(tbl.lookup(pred));
    CHECK_EQUAL(rank(result), 53u);
    auto check_uid = [](const event& e, const std::string& uid) {
      auto& v = get<vector>(e.data());
      return v[1] == uid;
    };
    for (auto i : select(result))
      if (i == 819)
        CHECK(check_uid(bro_conn_log[819], "KKSlmtmkkxf")); // first
      else if (i == 3594)
        CHECK(check_uid(bro_conn_log[3594], "GDzpFiROJQi")); // intermediate
      else if (i == 6338)
        CHECK(check_uid(bro_conn_log[6338], "zwCckCCgXDb")); // last
    MESSAGE("(automatically) persist table index to disk");
  }
  { // lifetime scope of the second table index
    MESSAGE("restore table index from disk");
    auto tbl = unbox(make_table_index(directory, tbl_type));
    MESSAGE("verify table index again");
    auto result = unbox(tbl.lookup(pred));
    CHECK_EQUAL(rank(result), 53u);
    auto check_uid = [](const event& e, const std::string& uid) {
      auto& v = get<vector>(e.data());
      return v[1] == uid;
    };
    for (auto i : select(result))
      if (i == 819)
        CHECK(check_uid(bro_conn_log[819], "KKSlmtmkkxf")); // first
      else if (i == 3594)
        CHECK(check_uid(bro_conn_log[3594], "GDzpFiROJQi")); // intermediate
      else if (i == 6338)
        CHECK(check_uid(bro_conn_log[6338], "zwCckCCgXDb")); // last
  }
}

FIXTURE_SCOPE_END()
