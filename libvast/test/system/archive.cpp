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
#include "vast/ids.hpp"
#include "vast/system/archive.hpp"
#include "vast/table_slice.hpp"

#include "vast/detail/spawn_container_source.hpp"

#define SUITE archive
#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  system::archive_type a;

  fixture() {
    a = self->spawn(system::archive, directory, 10, 1024 * 1024);
  }

  template <class T>
  void push_to_archive(std::vector<T> xs) {
    vast::detail::spawn_container_source(sys, std::move(xs), a);
    run();
  }

  std::vector<event> query(std::initializer_list<id_range> ranges) {
    auto ids = make_ids(ranges);
    return request<std::vector<event>>(a, ids);
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(archive_tests, fixture)

TEST(bro conn logs slices) {
  push_to_archive(bro_conn_log_slices);
  auto result = query({{10, 15}});
  CHECK_EQUAL(result.size(), 5u);
}

TEST(archiving and querying) {
  MESSAGE("import bro conn logs to archive");
  push_to_archive(bro_conn_log_slices);
  MESSAGE("import DNS logs to archive");
  push_to_archive(bro_dns_log_slices);
  MESSAGE("import HTTP logs to archive");
  push_to_archive(bro_http_log_slices);
  MESSAGE("import BGP dump logs to archive");
  push_to_archive(bgpdump_txt_slices);
  MESSAGE("query events");
  auto ids = make_ids({{24, 56}, {1076, 1096}});
  auto result = request<std::vector<event>>(a, ids);
  REQUIRE_EQUAL(result.size(), 52u);
  // We sort because the specific compression algorithm used at the archive
  // determines the order of results.
  std::sort(result.begin(), result.end());
  // We processed the segments in reverse order of arrival (to maximize LRU hit
  // rate). Therefore, the result set contains first the events with higher
  // IDs [10150,10200) and then the ones with lower ID [100,150).
  CHECK_EQUAL(result[0].id(), 24u);
  CHECK_EQUAL(result[0].type().name(), "bro::dns");
  CHECK_EQUAL(result[32].id(), 1076u);
  CHECK_EQUAL(result[32].type().name(), "bro::http");
  CHECK_EQUAL(result[result.size() - 1].id(), 1095u);
  self->send_exit(a, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
