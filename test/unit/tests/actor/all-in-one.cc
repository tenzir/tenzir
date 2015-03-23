#include <caf/all.hpp>

#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/configuration.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/query_options.h"
#include "vast/actor/program.h"
#include "vast/actor/task.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("all-in-one")
{
  VAST_INFO("importing a single Bro log");
  configuration import;
  *import["tracker.port"] = 42002;
  *import['I'] = "bro";
  *import['C'] = true;
  *import['r'] = m57_day11_18::ssl;
  *import["import.batch-size"] = 10;
  *import["archive.max-segment-size"] = 1;
  REQUIRE(import.verify());
  path dir = *import.get("directory");
  if (exists(dir))
    REQUIRE(rm(dir));
  scoped_actor self;
  self->send(self->spawn<program>(import), run_atom::value);
  self->await_all_other_actors_done();

  VAST_INFO("restarting a new core");
  configuration core_config;
  *core_config['C'] = true;
  *core_config["tracker.port"] = 42003;
  REQUIRE(core_config.verify());
  auto core = spawn<program>(core_config);
  self->send(core, run_atom::value);

  VAST_INFO("testing whether archive has the correct chunk");
  actor tracker;
  self->sync_send(core, tracker_atom::value).await([&](actor const& t)
  {
    tracker = t;
  });
  self->send(tracker, get_atom::value, *core_config.get("archive.name"));
  self->receive([&](actor const& a) { self->send(a, event_id{112}); });
  self->receive([&](chunk const& chk)
  {
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

  VAST_INFO("testing whether a manual index lookup succeeds");
  auto pops = to<expression>("id.resp_p == 995/?");
  REQUIRE(pops);
  self->send(tracker, get_atom::value, *core_config.get("index.name"));
  self->receive([&](actor const& index)
  {
    self->send(index, *pops, historical, self);
  });
  self->receive([&](actor const& task)
  {
    self->send(task, subscriber_atom::value, self);
  });
  uint64_t left = 5;
  self->do_receive(
    [&](default_bitstream const& hits)
    {
      CHECK(hits.count() > 0);
    },
    [&](done_atom, time::extent, expression const& expr)
    {
      CHECK(expr == *pops);
    },
    [&](progress_atom, uint64_t remaining, uint64_t total)
    {
      CHECK(total == 5);
      CHECK(--left == remaining);
    }).until([&left] { return left == 0; });

  VAST_INFO("constructing a simple POPS query");
  self->send(tracker, get_atom::value, *core_config.get("search.name"));
  self->receive([&](actor search)
  {
    auto q = "id.resp_p == 995/?";
    self->sync_send(search, q, historical, self).await(
      [&](expression const& ast, actor const& qry)
      {
        CHECK(ast == *pops);
        self->send(qry, extract_atom::value, uint64_t{46});
      });
  });

  VAST_INFO("checking POPS query results");
  auto i = 0;
  self->receive_for(i, 46) (
    [&](event const& e)
    {
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
    });

  VAST_INFO("waiting on final done from QUERY");
  self->receive([&](done_atom, time::extent) { REQUIRE(true); });
  self->send_exit(core, exit::done);
  self->await_all_other_actors_done();

  VAST_INFO("importing another Bro log");
  *import["import.batch-size"] = 100;
  *import['r'] = m57_day11_18::conn;
  self->send(self->spawn<program>(import), run_atom::value);
  self->await_all_other_actors_done();

  VAST_INFO("restarting another core");
  core = spawn<program>(core_config);
  self->send(core, run_atom::value);

  VAST_INFO("issuing query against conn and ssl");
  self->sync_send(core, tracker_atom::value).await([&](actor const& t)
  {
    tracker = t;
  });
  self->send(tracker, get_atom::value, *core_config.get("search.name"));
  self->receive(
    [&](actor const& search)
    {
      auto q = "id.resp_p == 443/? && \"mozilla\" in ssl.server_name";
      self->sync_send(search, q, historical, self).await(
        [&](expression const&, actor const& qry)
        {
          // Extract all results.
          self->send(qry, extract_atom::value, uint64_t{0});
          self->monitor(qry);
        });
    });

  VAST_INFO("processing query results");
  auto done = false;
  size_t n = 0;
  self->do_receive(
    [&](event const&)
    {
      ++n;
    },
    [=](progress_atom, double, uint64_t)
    {
      REQUIRE(true);
    },
    [&](done_atom, time::extent)
    {
      CHECK(n == 15);
    },
    [&](down_msg const& d)
    {
      // Query terminates after having extracted all events.
      CHECK(d.reason == exit::done);
      done = true;
    }).until([&done] { return done; });

  self->send_exit(core, exit::done);
  self->await_all_other_actors_done();

  VAST_INFO("removing temporary directory");
  CHECK(rm(*core_config.get("directory")));
}
