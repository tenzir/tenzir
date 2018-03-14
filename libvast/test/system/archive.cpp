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

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/system/archive.hpp"
#include "vast/ids.hpp"

#define SUITE archive
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(archive_tests, fixtures::actor_system_and_events)

TEST(archiving and querying) {
  auto a = self->spawn(system::archive, directory, 10, 1024 * 1024);
  MESSAGE("sending events");
  self->send(a, bro_conn_log);
  self->send(a, bro_dns_log);
  self->send(a, bro_http_log);
  self->send(a, bgpdump_txt);
  MESSAGE("querying events");
  auto ids = make_ids({{100, 150}, {10150, 10200}});
  std::vector<event> result;
  self->request(a, infinite, ids).receive(
    [&](std::vector<event>& xs) { result = std::move(xs); },
    error_handler()
  );
  REQUIRE_EQUAL(result.size(), 100u);
  // We sort because the specific compression algorithm used at the archive
  // determines the order of results.
  std::sort(result.begin(), result.end());
  // We processed the segments in reverse order of arrival (to maximize LRU hit
  // rate). Therefore, the result set contains first the events with higher
  // IDs [10150,10200) and then the ones with lower ID [100,150).
  CHECK_EQUAL(result[0].id(), 100u);
  CHECK_EQUAL(result[0].type().name(), "bro::conn");
  CHECK_EQUAL(result[50].id(), 10150u);
  CHECK_EQUAL(result[50].type().name(), "bro::dns");
  CHECK_EQUAL(result[result.size() - 1].id(), 10199u);
  self->send_exit(a, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
