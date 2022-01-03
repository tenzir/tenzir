//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE expression

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/ids.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

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
  type id_type
    = caf::get<record_type>(zeek_conn_log[0].layout()).field(offset{1}).type;
};

} // namespace

FIXTURE_SCOPE(evaluation_tests, fixture)

TEST(evaluation - meta extractor - #type) {
  auto expr = make_expr("#type == \"zeek.conn\"");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  CHECK_EQUAL(ids, make_ids({{0, zeek_conn_log_slice.rows()}}));
}

TEST(evaluation - meta extractor - #field) {
  auto expr = make_expr("#field == \"a.b.c\"");
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
  REQUIRE_EQUAL(rank(ids), 1u);
  auto id = select(ids, 1);
  REQUIRE_EQUAL(id, 97u);
  CHECK_EQUAL(zeek_conn_log_slice.at(id, 1), make_data_view("jM8ATYNKqZg"));
}

TEST(evaluation - field extractor - orig_h + proto) {
  // head -n 108 conn.log | awk '$3 != "192.168.1.102" && $7 != "udp"'
  auto expr = make_conn_expr("orig_h != 192.168.1.102 && proto != \"udp\"");
  auto ids = evaluate(expr, zeek_conn_log_slice);
  REQUIRE_EQUAL(rank(ids), 10u);
  auto last = select(ids, -1);
  CHECK_EQUAL(zeek_conn_log_slice.at(last, 1), make_data_view("WfzxgFx2lWb"));
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
