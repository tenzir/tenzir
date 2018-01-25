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

#include "vast/ids.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/query_options.hpp"

#include "vast/system/index.hpp"

#define SUITE index
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace std::chrono;

namespace {

} // namespace <anonymous>

FIXTURE_SCOPE(index_tests, fixtures::actor_system_and_events)

TEST(index) {
  directory /= "index";
  MESSAGE("spawing");
  auto index = self->spawn(system::index, directory, 1000, 5, 10);
  MESSAGE("indexing logs");
  self->send(index, bro_conn_log);
  self->send(index, bro_dns_log);
  self->send(index, bro_http_log);
  MESSAGE("issueing queries");
  auto expr = to<expression>(":addr == 74.125.19.100");
  REQUIRE(expr);
  auto total_hits = size_t{11u + 0 + 24}; // conn + dns + http
  // An index lookup first returns a unique ID along with the number of
  // partitions that the expression spans. In
  // case the lookup doesn't yield any hits, the index returns the invalid
  // (nil) ID.
  self->send(index, *expr);
  self->receive(
    [&](const uuid& id, size_t total, size_t scheduled) {
      CHECK_NOT_EQUAL(id, uuid::nil());
      // Each batch wound up in its own partition.
      CHECK_EQUAL(total, 3u);
      CHECK_EQUAL(scheduled, 3u);
      // After the lookup ID has arrived,
      size_t i = 0;
      ids all;
      self->receive_for(i, scheduled)(
        [&](const ids& hits) { all |= hits; },
        error_handler()
      );
      CHECK_EQUAL(rank(all), total_hits);
    },
    error_handler()
  );
  self->send_exit(index, exit_reason::user_shutdown);
  self->wait_for(index);
  CHECK(exists(directory / "meta"));
  MESSAGE("reloading index");
  index = self->spawn(system::index, directory, 1000, 2, 2);
  MESSAGE("issueing queries");
  self->send(index, *expr);
  self->receive(
    [&](const uuid& id, size_t total, size_t scheduled) {
      CHECK_NOT_EQUAL(id, uuid::nil());
      CHECK_EQUAL(total, 3u);
      CHECK_EQUAL(scheduled, 2u); // Only two this time
      size_t i = 0;
      ids all;
      self->receive_for(i, scheduled)(
        [&](const ids& hits) { all |= hits; },
        error_handler()
      );
      // Evict one partition.
      self->send(index, id, size_t{1});
      self->receive(
        [&](const ids& hits) { all |= hits; },
        error_handler()
      );
      CHECK_EQUAL(rank(all), total_hits); // conn + dns + http
    },
    error_handler()
  );
  self->send_exit(index, exit_reason::user_shutdown);
  self->wait_for(index);
}

FIXTURE_SCOPE_END()
