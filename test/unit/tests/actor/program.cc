#include <caf/all.hpp>

#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/configuration.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/actor/program.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("basic actor integrity")
{
  // First spawn the core.
  configuration core_config;
  *core_config["tracker.port"] = 42002;
  *core_config['v'] = 0;
  *core_config['V'] = 5;
  *core_config['C'] = true;
  REQUIRE(core_config.verify());

  path dir = *core_config.get("directory");
  if (exists(dir))
    REQUIRE(rm(dir));

  auto core = spawn<program>(core_config);
  anon_send(core, atom("run"));

  // Wait until the TCP sockets of the core have bound.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Import a single Bro log.
  configuration import_config;
  *import_config["tracker.port"] = 42002;
  *import_config['v'] = 0;
  *import_config['V'] = 5;
  *import_config['I'] = "bro";
  *import_config['r'] = m57_day11_18::ssl;
  *import_config["import.batch-size"] = 10;
  *import_config["archive.max-segment-size"] = 1;
  REQUIRE(import_config.verify());

  auto import = spawn<program>(import_config);
  import->link_to(core); // Pull down core after import.
  anon_send(import, atom("run"));

  await_all_actors_done();

  // Restart a new core.
  *core_config["tracker.port"] = 42003;
  *core_config['v'] = 0;
  *core_config['V'] = 5;
  *core_config['C'] = true;
  REQUIRE(core_config.verify());

  core = spawn<program>(core_config);
  anon_send(core, atom("run"));

  scoped_actor self;
  auto fail = others() >> [&]
  {
    std::cerr
      << "unexpected message from " << self->last_sender().id() << ": "
      << to_string(self->last_dequeued()) << std::endl;

    REQUIRE(false);
  };

  //
  // Test whether the archive has the correct chunk.
  //
  actor trackr;
  self->send(core, atom("tracker"));
  self->receive([&](actor const& t) { trackr = t; });

  self->send(trackr, atom("get"), *core_config.get("archive.name"));
  self->receive([&](actor const& a) { self->send(a, event_id(112)); });
  self->receive(
      on_arg_match >> [&](chunk const& chk)
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
      },
      fail);

  //
  // Test whether a manual index lookup succeeds.
  //
  auto pops = to<expression>("id.resp_p == 995/?");
  REQUIRE(pops);

  self->send(trackr, atom("get"), *core_config.get("index.name"));
  self->receive(
      [&](actor index) { self->send(index, atom("query"), *pops, self); });

  bool done = false;
  self->do_receive(
      [&](bitstream const& hits)
      {
        CHECK(hits.count() > 0);
      },
      on(atom("progress"), arg_match) >> [&](double progress, uint64_t hits)
      {
        if (progress == 1.0)
        {
          done = true;
          CHECK(hits == 46);
        }
      },
      fail
      ).until([&done] { return done; });

  //
  // Construct a simple query and verify that the results are correct.
  //
  self->send(trackr, atom("get"), *core_config.get("search.name"));
  self->receive(
      [&](actor search)
      {
        auto q = "id.resp_p == 995/?";
        self->sync_send(search, atom("query"), self, q).await((
            [&](expression const& ast, actor qry)
            {
              CHECK(ast == *pops);
              self->send(qry, atom("extract"), uint64_t{46});
            },
            fail));
      },
      fail);

  self->receive(
      on(atom("progress"), arg_match) >> [&](double progress, uint64_t hits)
      {
        CHECK(progress == 1.0);
        CHECK(hits == 46);
      },
      fail);

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
    },
    fail);

  // A query always sends a "done" atom before terminating.
  self->receive(
      on(atom("done")) >> [&] { REQUIRE(true); },
      fail);

  // Now import another Bro log.
  *import_config["tracker.port"] = 42003;
  *import_config['r'] = m57_day11_18::conn;
  import = self->spawn<program, monitored>(import_config);
  anon_send(import, atom("run"));
  self->receive(
      on_arg_match >> [&](down_msg const& d) { CHECK(d.reason == exit::done); },
      fail);

  // Wait for the segment to arrive at the receiver.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  self->send(trackr, atom("get"), *core_config.get("index.name"));
  self->receive(
      [&](actor index)
      {
        self->sync_send(index, atom("flush")).await(
            on_arg_match >> [&](actor task_tree)
            {
              anon_send(task_tree, atom("notify"), self);
              self->receive(
                  on(atom("done")) >> [&]
                  {
                    CHECK(self->last_sender() == task_tree);
                    path part;
                    for (auto& p : directory{dir / "index"})
                      if (p.is_directory())
                      {
                        part = p;
                        break;
                      }

                    REQUIRE(! part.empty());
                    REQUIRE(exists(part / "types" / "conn"));
                  },
                  fail);
            },
            fail);
      });

  // Issue a query against both conn and ssl.
  self->send(trackr, atom("get"), *core_config.get("search.name"));
  self->receive(
      [&](actor search)
      {
        auto q = "id.resp_p == 443/? && \"mozilla\" in ssl.server_name";
        self->sync_send(search, atom("query"), self, q).await((
            [&](expression const&, actor qry)
            {
              // Extract all results.
              self->send(qry, atom("extract"), uint64_t{0});
              self->monitor(qry);
            },
            fail));
      },
      fail);

  done = false;
  size_t n = 0;
  self->do_receive(
      [&](event const&)
      {
        ++n;
      },
      on(atom("progress"), arg_match) >> [=](double, uint64_t)
      {
        REQUIRE(true);
      },
      on(atom("done")) >> [&]
      {
        CHECK(n == 15);
      },
      [&](down_msg const& d)
      {
        // Query terminates after having extracted all events.
        CHECK(d.reason == exit::done);
        done = true;
      },
      fail
      ).until([&done] { return done; });

  self->send_exit(core, exit::done);
  self->await_all_other_actors_done();

  // Give the OS some time to flush to the filsystem.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  CHECK(rm(*core_config.get("directory")));
}
