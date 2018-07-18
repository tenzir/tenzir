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

#include "vast/bitmap.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/const_table_slice_handle.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/table_index.hpp"
#include "vast/table_slice_handle.hpp"

#define SUITE table_index
#include "test.hpp"

#include "fixtures/events.hpp"
#include "fixtures/filesystem.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

struct fixture : fixtures::events, fixtures::filesystem {
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

  void add(const_table_slice_handle x) {
    auto err = tbl->add(x);
    if (err)
      FAIL("error: " << err);
  }

  std::unique_ptr<table_index> tbl;
};

} // namespace <anonymous>

FIXTURE_SCOPE(table_index_tests, fixture)

TEST(integer values) {
  MESSAGE("generate table layout for flat integer type");
  integer_type column_type;
  record_type layout{{"value", column_type}};
  reset(make_table_index(directory, layout));
  MESSAGE("ingest test data (integers)");
  auto rows = make_rows(1, 2, 3, 1, 2, 3, 1, 2, 3);
  auto slice = default_table_slice::make(layout, rows);
  REQUIRE_NOT_EQUAL(slice.get(), nullptr);
  REQUIRE_EQUAL(slice->columns(), 1u);
  REQUIRE_EQUAL(slice->rows(), rows.size());
  add(slice);
  auto res = [&](auto... args) {
    return make_ids({args...}, rows.size());
  };
  MESSAGE("verify table index");
  auto verify = [&] {
    CHECK_EQUAL(query("value == +1"), res(0, 3, 6));
    CHECK_EQUAL(query(":int == +1"), res(0, 3, 6));
    CHECK_EQUAL(query(":int == +2"), res(1, 4, 7));
    CHECK_EQUAL(query(":int == +3"), res(2, 5, 8));
    CHECK_EQUAL(query(":int == +4"), res());
    CHECK_EQUAL(query(":int != +1"), res(1, 2, 4, 5, 7, 8));
    CHECK_EQUAL(query("!(:int == +1)"), res(1, 2, 4, 5, 7, 8));
    CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1, 4, 7));
  };
  verify();
  MESSAGE("(automatically) persist table index and restore from disk");
  reset(make_table_index(directory, layout));
  MESSAGE("verify table index again");
  verify();
}

TEST(record type) {
  MESSAGE("generate table layout for record type");
  record_type layout {
    {"x.a", integer_type{}},
    {"x.b", boolean_type{}},
    {"y.a", string_type{}},
  };
  reset(make_table_index(directory, layout));
  MESSAGE("ingest test data (records)");
  auto mk_row = [&](int x, bool y, std::string z) {
    return vector{x, y, std::move(z)};
  };
  // Some test data.
  std::vector<vector> rows{mk_row(1, true, "abc"),     mk_row(10, false, "def"),
                           mk_row(5, true, "hello"),   mk_row(1, true, "d e f"),
                           mk_row(15, true, "world"),  mk_row(5, true, "bar"),
                           mk_row(10, false, "a b c"), mk_row(10, false, "baz"),
                           mk_row(5, false, "foo"),    mk_row(1, true, "test")};
  auto slice = default_table_slice::make(layout, rows);
  REQUIRE_EQUAL(slice->rows(), rows.size());
  REQUIRE_EQUAL(slice->columns(), 3u);
  add(slice);
  auto res = [&](auto... args) {
    return make_ids({args...}, rows.size());
  };
  MESSAGE("verify table index");
  auto verify = [&] {
    CHECK_EQUAL(query("x.a == +1"), res(0, 3, 9));
    CHECK_EQUAL(query("x.a > +1"), res(1, 2, 4, 5, 6, 7, 8));
    CHECK_EQUAL(query("x.a > +1 && x.b == T"), res(2, 4, 5));
  };
  verify();
  MESSAGE("(automatically) persist table index and restore from disk");
  reset(make_table_index(directory, layout));
  MESSAGE("verify table index again");
  verify();
}

TEST(bro conn logs) {
  MESSAGE("generate table layout for bro conn logs");
  auto layout = bro_conn_log_layout();
  reset(make_table_index(directory, layout));
  MESSAGE("ingest test data (bro conn log)");
  for (auto slice : const_bro_conn_log_slices)
    add(slice);
  MESSAGE("verify table index");
  auto verify = [&] {
    CHECK_EQUAL(rank(query("id.resp_p == 995/?")), 53u);
    CHECK_EQUAL(rank(query("id.resp_p == 5355/?")), 49u);
    CHECK_EQUAL(rank(query("id.resp_p == 995/? || id.resp_p == 5355/?")), 102u);
    CHECK_EQUAL(rank(query("&time > 1970-01-01")), bro_conn_log.size());
    CHECK_EQUAL(rank(query("proto == \"udp\"")), 5306u);
    CHECK_EQUAL(rank(query("proto == \"tcp\"")), 3135u);
    CHECK_EQUAL(rank(query("uid == \"nkCxlvNN8pi\"")), 1u);
    CHECK_EQUAL(rank(query("orig_bytes < 400")), 5332u);
    CHECK_EQUAL(rank(query("orig_bytes < 400 && proto == \"udp\"")), 4357u);
    CHECK_EQUAL(rank(query(":addr == 169.254.225.22")), 4u);
    CHECK_EQUAL(rank(query("service == \"http\"")), 2386u);
    CHECK_EQUAL(rank(query("service == \"http\" && :addr == 212.227.96.110")),
                28u);
  };
  verify();
  MESSAGE("(automatically) persist table index and restore from disk");
  reset(make_table_index(directory, layout));
  MESSAGE("verify table index again");
  verify();
}

TEST(bro conn log http slices) {
  MESSAGE("scrutinize each bro conn log slice individually");
  // Pre-computed via:
  // grep http libvast/test/logs/bro/conn.log  -n
  // | awk -F ':' '{tbl[int($1 / 100)] += 1}
  //               END { for (key in tbl) { print key " " tbl[key] } }'
  // | sort -n
  // | awk '{print $2","}'
  std::vector<size_t> hits{
    9,  20, 14, 28, 31, 7,  15, 28, 16, 41, 40, 51, 61, 50, 65, 58, 54,
    24, 26, 30, 20, 30, 8,  57, 48, 57, 30, 55, 22, 25, 34, 35, 40, 59,
    40, 23, 31, 26, 27, 53, 26, 5,  56, 35, 1,  5,  7,  10, 4,  44, 48,
    2,  9,  7,  1,  13, 4,  2,  13, 2,  33, 36, 16, 43, 50, 30, 38, 13,
    92, 70, 73, 67, 5,  53, 21, 8,  2,  2,  22, 7,  2,  14, 7,
  };
  auto layout = bro_conn_log_layout();
  REQUIRE_EQUAL(std::accumulate(hits.begin(), hits.end(), size_t(0)), 2386u);
  for (size_t slice_id = 0; slice_id < hits.size(); ++slice_id) {
    tbl.reset();
    rm(directory);
    reset(make_table_index(directory, layout));
    add(const_bro_conn_log_slices[slice_id]);
    CHECK_EQUAL(rank(query("service == \"http\"")), hits[slice_id]);
  }
}

FIXTURE_SCOPE_END()
