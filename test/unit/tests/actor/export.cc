#include <caf/all.hpp>

#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/query_options.h"
#include "vast/uuid.h"
#include "vast/actor/task.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/expression.h"

#define SUITE actors
#include "test.h"
#include "data.h"
#include "fixtures/core.h"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(core_scope, fixtures::core)

TEST(export)
{
  MESSAGE("inhaling a Bro SSL log");
  auto n = make_core();
  run_source(n, "bro", "-b", "10", "-r", m57_day11_18::ssl);
  stop_core(n);
  self->await_all_other_actors_done();

  MESSAGE("testing whether archive has the correct chunk");
  n = make_core();
  self->sync_send(n, get_atom::value, "archive").await(
    [&](actor const& a, std::string const& fqn, std::string const& type)
    {
      CHECK(fqn == "archive@" + node_name);
      CHECK(type == "archive");
      REQUIRE(a != invalid_actor);
      self->send(a, event_id{112});
    }
  );
  self->receive([&](chunk const& chk)
  {
    MESSAGE("checking chunk integrity");
    // The ssl.log has a total of 113 events and we use batches of 10. So
    // the last chunk has three events in [110, 112].
    CHECK(chk.meta().ids.find_first() == 110);
    CHECK(chk.meta().ids.find_last() == 112);
    // Check the last ssl.log entry.
    chunk::reader r{chk};
    auto e = r.read(112);
    REQUIRE(e);
    CHECK(get<record>(*e)->at(1) == "XBy0ZlNNWuj");
    CHECK(get<record>(*e)->at(3) == "TLSv10");
  });

  MESSAGE("performing manual index lookup");
  auto pops = vast::detail::to_expression("id.resp_p == 995/?");
  REQUIRE(pops);
  self->sync_send(n, get_atom::value, "index").await(
    [&](actor const& a, std::string const& fqn, std::string const& type)
    {
      CHECK(fqn == "index@" + node_name);
      CHECK(type == "index");
      REQUIRE(a != invalid_actor);
      self->send(a, *pops, historical, self);
    }
  );
  MESSAGE("retrieving lookup task");
  self->receive([&](actor const& task)
  {
    self->send(task, subscriber_atom::value, self);
  });
  MESSAGE("getting hits");
  auto done = false;
  self->do_receive(
    [&](default_bitstream const& hits)
    {
      CHECK(hits.count() > 0);
    },
    [&](done_atom, time::extent, expression const& expr)
    {
      done = true;
      CHECK(expr == *pops);
    },
    [&](progress_atom, uint64_t remaining, uint64_t total)
    {
      // The task we receive from INDEX consists of 12 stages, because we
      // imported the ssl.log with 113 entries in batches of 10, which yields
      // 11 full partitions and 1 partial one of 3, i.e., 11 + 1 = 12.
      if (remaining == 0)
        CHECK(total == 12);
    },
    others() >> [&]
    {
      ERROR("got unexpected message from " << self->current_sender() <<
            ": " << to_string(self->current_message()));
    }).until([&] { return done; });

  MESSAGE("performing index lookup via exporter");
  std::vector<message> msgs = {
    make_message("connect", "exporter", "archive"),
    make_message("connect", "exporter", "index")
  };
  actor exp;
  self->sync_send(n, "spawn", "exporter", "-h", "id.resp_p == 995/?").await(
    [&](actor const& a) {
      exp = a;
    },
    [&](error const& e) {
      ERROR(e);
      REQUIRE(false);
    }
  );
  REQUIRE(exp != invalid_actor);
  for (auto& msg : msgs)
    self->sync_send(n, msg).await([](ok_atom) {});
  self->send(exp, put_atom::value, sink_atom::value, self);
  self->send(exp, run_atom::value);
  self->send(exp, extract_atom::value, max_events);
  MESSAGE("verifying query results");
  auto i = 0;
  done = false;
  self->do_receive(
    [&](uuid const&, event const& e)
    {
      ++i;
      // Verify contents of a few random events.
      if (e.id() == 3)
        CHECK(get<record>(e)->at(1) == "KKSlmtmkkxf");
      if (e.id() == 41)
      {
        CHECK(get<record>(e)->at(1) == "7e0gZmKgGS4");
        CHECK(get<record>(e)->at(4) == "TLS_RSA_WITH_RC4_128_MD5");
      }
      // The last event.
      if (e.id() == 102)
        CHECK(get<record>(e)->at(1) == "mXRBhfuUqag");
    },
    [&](uuid const&, progress_atom, double, uint64_t)
    {
      // nop
    },
    [&](uuid const&, done_atom, time::extent)
    {
      CHECK(i == 46);
      done = true;
    },
    others() >> [&]
    {
      ERROR("got unexpected message from " << self->current_sender() <<
            ": " << to_string(self->current_message()));
    }).until([&] { return done; });

  stop_core(n);
  self->await_all_other_actors_done();

  MESSAGE("importing another Bro log (conn)");
  n = make_core();
  run_source(n, "bro", "-b", "100", "-r", m57_day11_18::conn);
  stop_core(n);
  self->await_all_other_actors_done();

  MESSAGE("issuing query against conn.log and ssl.log");
  n = make_core();
  msgs = {
    make_message("connect", "exporter", "archive"),
    make_message("connect", "exporter", "index")
  };
  auto q = "id.resp_p == 443/? && \"mozilla\" in bro::ssl.server_name";
  exp = invalid_actor;
  self->sync_send(n, "spawn", "exporter", "-h", q).await(
    [&](actor const& a) {
      exp = a;
    },
    [&](error const& e) {
      ERROR(e);
      REQUIRE(false);
    }
  );
  REQUIRE(exp != invalid_actor);
  for (auto& msg : msgs)
    self->sync_send(n, msg).await([](ok_atom) {});
  self->send(exp, put_atom::value, sink_atom::value, self);
  self->send(exp, run_atom::value);
  self->send(exp, extract_atom::value, max_events);
  self->monitor(exp);
  MESSAGE("processing query results");
  i = 0;
  done = false;
  self->do_receive(
    [&](uuid const&, event const&)
    {
      ++i;
    },
    [&](uuid const&, progress_atom, double, uint64_t)
    {
      // nop
    },
    [&](uuid const&, done_atom, time::extent)
    {
      CHECK(i == 15);
    },
    [&](down_msg const& msg)
    {
      // Query terminates after having extracted all events.
      CHECK(msg.reason == exit::done);
      done = true;
    },
    others() >> [&]
    {
      ERROR("got unexpected message from " << self->current_sender() <<
            ": " << to_string(self->current_message()));
    }).until([&done] { return done; });

  stop_core(n);
}

FIXTURE_SCOPE_END()
