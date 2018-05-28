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

#include "vast/system/column_index.hpp"

using namespace vast::system;

namespace {

struct fixture : fixtures::events, fixtures::filesystem {
  fixture() {
    directory /= "column-index";
  }

  void add_col(caf::expected<column_index_ptr> x) {
    REQUIRE(x);
    cols.emplace_back(std::move(*x));
  }

  std::vector<column_index_ptr> cols;
};

} // namespace <anonymous>

FIXTURE_SCOPE(column_index_tests, fixture)

TEST(todo) {
  MESSAGE("generate column layout for bro conn logs");
  const auto conn_log_type = bro_conn_log[0].type();
  /*
  auto ecols = column_index::make_default_indexes(directory, bro_conn_log);
  REQUIRE(ecols);
  auto cols = std::move(*ecols);
  MESSAGE("ingesting events");
  for (auto& entry : bro_conn_log)
    add_to_index(cols, entry);
  // Event indexers operate with predicates, whereas partitions take entire
  // expressions.
  MESSAGE("querying data using predicate 'id.resp_p == 995/?'");
  auto pred = to<predicate>("id.resp_p == 995/?");
  REQUIRE(pred);
  bitmap result;
  self->request(i, infinite, *pred).receive(
    [&](bitmap& bm) {
      result = std::move(bm);
    },
    error_handler()
  );
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
  MESSAGE("shutting down indexer");
  self->send(i, system::shutdown_atom::value);
  self->wait_for(i);
  CHECK(exists(directory));
  CHECK(exists(directory / "data" / "id" / "orig_h"));
  CHECK(exists(directory / "meta" / "time"));
  MESSAGE("respawning indexer from file system");
  i = self->spawn(system::event_indexer, directory, conn_log_type);
  // Same as above: submit the query and verify the result.
  self->request(i, infinite, *pred).receive(
    [&](bitmap& bm) {
      CHECK_EQUAL(rank(bm), 53u);
    },
    error_handler()
  );
  // Test another query that involves a map-reduce computation of value
  // indexers.
  pred = to<predicate>(":addr == 65.55.184.16");
  REQUIRE(pred);
  self->request(i, infinite, *pred).receive(
    [&](const bitmap& bm) {
      CHECK_EQUAL(rank(bm), 2u);
    },
    error_handler()
  );
  */
}

FIXTURE_SCOPE_END()
