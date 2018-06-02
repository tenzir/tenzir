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
  template <class T>
  T unbox(expected<T> x) {
    if (!x)
      FAIL("error: " << x.error());
    return std::move(*x);
  }

  ids query(std::string_view what) {
    return unbox(tbl->lookup(unbox(to<expression>(what))));
  }

  void reset(table_index&& new_tbl) {
    tbl = std::make_unique<table_index>(std::move(new_tbl));
  }

  void reset(expected<table_index>&& new_tbl) {
    if (!new_tbl)
      FAIL("error: " << new_tbl.error());
    reset(std::move(*new_tbl));
  }

  void add(event x) {
    tbl->add(std::move(x));
  }

  std::unique_ptr<table_index> tbl;
};

} // namespace <anonymous>

FIXTURE_SCOPE(table_index_tests, fixture)

TEST(flat type) {
  MESSAGE("generate table layout for flat integer type");
  reset(make_table_index(directory, integer_type{}));
  MESSAGE("ingest test data (integers)");
  std::vector<int> xs{1, 2, 3, 1, 2, 3, 1, 2, 3};
  for (size_t i = 0; i < xs.size(); ++i) {
    event x{xs[i]};
    x.id(i);
    add(std::move(x));
  }
  auto res = [&](auto... args) {
    return make_ids({args...}, xs.size());
  };
  MESSAGE("verify table index");
  CHECK_EQUAL(query(":int == +1"), res(0, 3, 6));
  CHECK_EQUAL(query(":int == +2"), res(1, 4, 7));
  CHECK_EQUAL(query(":int == +3"), res(2, 5, 8));
  CHECK_EQUAL(query(":int == +4"), res());
  CHECK_EQUAL(query(":int != +1"), res(1, 2, 4, 5, 7, 8));
  CHECK_EQUAL(query("!(:int == +1)"), res(1, 2, 4, 5, 7, 8));
  CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1, 4, 7));
  MESSAGE("(automatically) persist table index and restore from disk");
  reset(make_table_index(directory, integer_type{}));
  MESSAGE("verify table index again");
  CHECK_EQUAL(query(":int == +1"), res(0, 3, 6));
  CHECK_EQUAL(query(":int == +2"), res(1, 4, 7));
  CHECK_EQUAL(query(":int == +3"), res(2, 5, 8));
  CHECK_EQUAL(query(":int == +4"), res());
  CHECK_EQUAL(query(":int != +1"), res(1, 2, 4, 5, 7, 8));
  CHECK_EQUAL(query("!(:int == +1)"), res(1, 2, 4, 5, 7, 8));
  CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1, 4, 7));
}

TEST(record type) {
  MESSAGE("generate table layout for record type");
  auto tbl_type = record_type{
    {"x", record_type{
      {"a", integer_type{}},
      {"b", boolean_type{}}
    }},
    {"y", record_type{
      {"a", string_type{}}
    }}
  };
  reset(make_table_index(directory, tbl_type));
  MESSAGE("ingest test data (records)");
  auto mk_row = [&](int x, bool y, std::string z) {
    return value::make(vector{vector{x, y}, vector{std::move(z)}}, tbl_type);
  };
  // Some test data.
  std::vector<value> xs{mk_row(1, true, "abc"),     mk_row(10, false, "def"),
                        mk_row(5, true, "hello"),   mk_row(1, true, "d e f"),
                        mk_row(15, true, "world"),  mk_row(5, true, "bar"),
                        mk_row(10, false, "a b c"), mk_row(10, false, "baz"),
                        mk_row(5, false, "foo"),    mk_row(1, true, "test")};
  for (size_t i = 0; i < xs.size(); ++i) {
    event x{xs[i]};
    x.id(i);
    add(std::move(x));
  }
  auto res = [&](auto... args) {
    return make_ids({args...}, xs.size());
  };
  MESSAGE("verify table index");
  CHECK_EQUAL(query("x.a == +1"), res(0, 3, 9));
  CHECK_EQUAL(query("x.a > +1"), res(1, 2, 4, 5, 6, 7, 8));
  CHECK_EQUAL(query("x.a > +1 && x.b == T"), res(2, 4, 5));
  MESSAGE("(automatically) persist table index and restore from disk");
  reset(make_table_index(directory, tbl_type));
  MESSAGE("verify table index again");
  CHECK_EQUAL(query("x.a == +1"), res(0, 3, 9));
  CHECK_EQUAL(query("x.a > +1"), res(1, 2, 4, 5, 6, 7, 8));
  CHECK_EQUAL(query("x.a > +1 && x.b == T"), res(2, 4, 5));
}

TEST(bro conn logs) {
  MESSAGE("generate table layout for bro conn logs");
  auto tbl_type = bro_conn_log[0].type();
  reset(make_table_index(directory, tbl_type));
  CHECK_EQUAL(tbl->num_meta_columns(), 2u);
  MESSAGE("ingest test data (bro conn log)");
  for (auto& entry : bro_conn_log) {
    add(entry);
  }
  MESSAGE("verify table index");
  CHECK_EQUAL(rank(query("id.resp_p == 995/?")), 53u);
  CHECK_EQUAL(rank(query("id.resp_p == 5355/?")), 49u);
  CHECK_EQUAL(rank(query("id.resp_p == 995/? || id.resp_p == 5355/?")), 102u);
  CHECK_EQUAL(rank(query("&time > 1970-01-01")), bro_conn_log.size());
  CHECK_EQUAL(rank(query("proto == \"udp\"")), 5306u);
  CHECK_EQUAL(rank(query("proto == \"tcp\"")), 3135u);
  CHECK_EQUAL(rank(query("uid == \"nkCxlvNN8pi\"")), 1u);
  CHECK_EQUAL(rank(query("orig_bytes < 400")), 5332u);
  CHECK_EQUAL(rank(query("orig_bytes < 400 && proto == \"udp\"")), 4357u);
  MESSAGE("(automatically) persist table index and restore from disk");
  reset(make_table_index(directory, tbl_type));
  MESSAGE("verify table index again");
  CHECK_EQUAL(rank(query("id.resp_p == 995/?")), 53u);
  CHECK_EQUAL(rank(query("id.resp_p == 5355/?")), 49u);
  CHECK_EQUAL(rank(query("id.resp_p == 995/? || id.resp_p == 5355/?")), 102u);
  CHECK_EQUAL(rank(query("&time > 1970-01-01")), bro_conn_log.size());
  CHECK_EQUAL(rank(query("proto == \"udp\"")), 5306u);
  CHECK_EQUAL(rank(query("proto == \"tcp\"")), 3135u);
  CHECK_EQUAL(rank(query("uid == \"nkCxlvNN8pi\"")), 1u);
  CHECK_EQUAL(rank(query("orig_bytes < 400")), 5332u);
  CHECK_EQUAL(rank(query("orig_bytes < 400 && proto == \"udp\"")), 4357u);
}

FIXTURE_SCOPE_END()
