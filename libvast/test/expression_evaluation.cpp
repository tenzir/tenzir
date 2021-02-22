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

#define SUITE expression

#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/ids.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

struct fixture : fixtures::events {
  fixture() {
    zeek_conn_log_slice = zeek_conn_log_full[0];
    zeek_conn_log_slice.offset(0); // make it easier to write tests
  }

  expression make_expr(std::string_view str) const {
    return unbox(to<expression>(str));
  }

  expression make_conn_expr(std::string_view str) const {
    auto expr = make_expr(str);
    return unbox(tailor(expr, zeek_conn_log_slice.layout()));
  }

  table_slice zeek_conn_log_slice;
  type id_type = unbox(zeek_conn_log[0].layout().at(offset{1})).type;
};

} // namespace

FIXTURE_SCOPE(evaluation_tests, fixture)

TEST(evaluation - attribute extractor - #timestamp) {
  auto expr = make_conn_expr("#timestamp <= 2009-11-18T08:09");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  CHECK_EQUAL(ids, make_ids({{0, 5}}, zeek_conn_log_slice.rows()));
}

TEST(evaluation - attribute extractor - #type) {
  auto expr = make_expr("#type == \"zeek.conn\"");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  CHECK_EQUAL(ids, make_ids({{0, zeek_conn_log_slice.rows()}}));
}

TEST(evaluation - attribute extractor - #foo) {
  auto expr = make_expr("#foo == 42");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  CHECK_EQUAL(ids.size(), zeek_conn_log_slice.rows());
  CHECK(all<0>(ids));
}

TEST(evaluation - type extractor - count) {
  // head -n 108 conn.log | grep '\t350\t' | wc -l
  auto expr = make_conn_expr(":count == 350");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  CHECK_EQUAL(rank(ids), 18u);
}

TEST(evaluation - type extractor - string + duration) {
  // head -n 108 conn.log | awk '$8 == "http" && $9 > 30'
  auto expr = make_conn_expr("\"http\" in :string && :duration > 30s");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  CHECK_EQUAL(rank(ids), 1u);
  auto id = select(ids, 1);
  REQUIRE_EQUAL(id, 97u);
  CHECK_EQUAL(zeek_conn_log_slice.at(id, 1, id_type), make_data_view("jM8ATYNKq"
                                                                     "Zg"));
}

TEST(evaluation - field extractor - orig_h + proto) {
  // head -n 108 conn.log | awk '$3 != "192.168.1.102" && $7 != "udp"'
  auto expr = make_conn_expr("orig_h != 192.168.1.102 && proto != \"udp\"");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  REQUIRE_EQUAL(rank(ids), 10u);
  auto last = select(ids, -1);
  CHECK_EQUAL(zeek_conn_log_slice.at(last, 1, id_type), make_data_view("WfzxgFx"
                                                                       "2lWb"));
}

TEST(evaluation - field extractor - service + orig_h) {
  auto str = "service == nil && orig_h == fe80::219:e3ff:fee7:5d23";
  auto expr = make_conn_expr(str);
  auto ids = evaluate(expr, zeek_conn_log_slice);
  REQUIRE_EQUAL(rank(ids), 2u);
}

TEST(evaluation - field extractor - nonexistant field) {
  auto expr = make_conn_expr("devnull != nil");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  CHECK(all<0>(ids));
}

FIXTURE_SCOPE_END()
