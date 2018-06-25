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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/bitmap.hpp"

#include "vast/system/indexer.hpp"

#define SUITE indexer
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  void init(type event_type) {
    indexer = self->spawn(system::indexer, directory, event_type);
    run_exhaustively();
  }

  void init(std::vector<event> events) {
    VAST_ASSERT(!events.empty());
    auto event_type = events.front().type();
    init(event_type);
    VAST_ASSERT(std::all_of(events.begin(), events.end(), [&](const event& x) {
      return x.type() == event_type;
    }));
    self->send(indexer, std::move(events));
    run_exhaustively();
  }

  ids query(std::string_view what) {
    auto pred = unbox(to<expression>(what));
    return request<ids>(indexer, std::move(pred));
  }

  actor indexer;
};

} // namespace <anonymous>

FIXTURE_SCOPE(indexer_tests, fixture)

TEST(integer rows) {
  MESSAGE("ingest integer events");
  integer_type layout;
  std::vector<int> ints{1, 2, 3, 1, 2, 3, 1, 2, 3};
  std::vector<event> events;
  for (auto i : ints)
    events.emplace_back(event::make(i, layout, events.size()));
  auto res = [&](auto... args) {
    return make_ids({args...}, events.size());
  };
  init(events);
  sched.run();
  MESSAGE("verify table index");
  auto verify = [&] {
    CHECK_EQUAL(query(":int == +1"), res(0, 3, 6));
    CHECK_EQUAL(query(":int == +2"), res(1, 4, 7));
    CHECK_EQUAL(query(":int == +3"), res(2, 5, 8));
    CHECK_EQUAL(query(":int == +4"), res());
    CHECK_EQUAL(query(":int != +1"), res(1, 2, 4, 5, 7, 8));
    CHECK_EQUAL(query("!(:int == +1)"), res(1, 2, 4, 5, 7, 8));
    CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1, 4, 7));
  };
  verify();
  MESSAGE("kill INDEXER");
  anon_send_exit(indexer, exit_reason::kill);
  run_exhaustively();
  MESSAGE("reload INDEXER from disk");
  init(layout);
  MESSAGE("verify table index again");
  verify();
}

TEST(bro conn logs) {
  MESSAGE("ingest bro conn log");
  init(bro_conn_log);
  MESSAGE("verify table index");
  auto res = [&](auto... args) {
    return make_ids({args...}, bro_conn_log.size());
  };
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
    CHECK_EQUAL(rank(query(":addr == 65.55.184.16")), 2u);
    CHECK_EQUAL(query(":addr == 169.254.225.22"), res(680, 682, 719, 720));
  };
  verify();
  MESSAGE("kill INDEXER");
  anon_send_exit(indexer, exit_reason::kill);
  run_exhaustively();
  MESSAGE("reload INDEXER from disk");
  init(bro_conn_log.front().type());
  MESSAGE("verify table index again");
  verify();
}

FIXTURE_SCOPE_END()
