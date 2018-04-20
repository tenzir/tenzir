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

#include "vast/detail/spawn_container_source.hpp"

#define SUITE archive
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(archive_tests, fixtures::deterministic_actor_system_and_events)

TEST(archiving and querying) {
  auto a = self->spawn(system::archive, directory, 10, 1024 * 1024);
  auto push_to_archive = [&](auto xs) {
    auto cs = vast::detail::spawn_container_source(sys, a, std::move(xs));
    run_exhaustively();
  };
  MESSAGE("import bro conn logs to archive");
  push_to_archive(bro_conn_log);
  MESSAGE("import DNS logs to archive");
  push_to_archive(bro_dns_log);
  MESSAGE("import HTTP logs to archive");
  push_to_archive(bro_http_log);
  MESSAGE("import BCP dump logs to archive");
  push_to_archive(bgpdump_txt);
  MESSAGE("query events");
  auto ids = make_ids({{100, 150}, {10150, 10200}});
  self->send(a, ids);
  run_exhaustively();
  auto result_opt = fetch_result<std::vector<event>>();
  REQUIRE(result_opt);
  auto& result = *result_opt;
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
