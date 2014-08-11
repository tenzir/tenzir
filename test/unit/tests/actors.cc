#include <caf/all.hpp>

#include "vast/bitstream.h"
#include "vast/bitmap_index.h"
#include "vast/configuration.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/program.h"
#include "vast/segment.h"
#include "vast/io/serialization.h"

#include "framework/unit.h"

#include "test/unit/bro_logs.h"

using namespace caf;
using namespace vast;

SUITE("actors")

namespace {

// Because we're running multiple TCP tests sequentially, we need to give the
// OS a bit of time to release the ports from previous test. This function sets
// different ports for different program instances as an alternative to waiting
// for the same ports to become free again.
void set_ports(configuration& config, uint64_t instance)
{
  uint16_t port = 42000 + instance * 5;
  *config["tracker.port"] = port++;
  *config["receiver.port"] = port++;
  *config["archive.port"] = port++;
  *config["index.port"] = port++;
  *config["search.port"] = port;
}

} // namespace <anonymous>

TEST("all-in-one import")
{
  configuration cfg;
  set_ports(cfg, 1);
  *cfg['v'] = 0;
  *cfg['V'] = 5;
  *cfg['C'] = true;
  *cfg['I'] = true;
  *cfg['r'] = m57_day11_18::ftp;
  *cfg['p'] = "m57_day11_18";

  REQUIRE(cfg.verify());

  anon_send(spawn<program>(cfg), atom("run"));
  await_all_actors_done();

  auto dir = path{*cfg.get("directory")};
  auto ftp = dir / "index" / "m57_day11_18" / "types" / "ftp";

  REQUIRE(exists(dir));
  REQUIRE(exists(ftp));

  uint64_t size;
  address_bitmap_index<default_bitstream> abmi;
  port_bitmap_index<default_bitstream> pbmi;

  REQUIRE(vast::io::unarchive(ftp / "id" / "orig_h" / "data.idx", size, abmi));
  REQUIRE(vast::io::unarchive(ftp / "id" / "orig_p" / "data.idx", size, pbmi));

  CHECK(size == 3); // Event ID 1 is the first valid ID.
  CHECK(size == abmi.size());

  auto eq = relational_operator::equal;
  auto orig_h = abmi.lookup(eq, *to<address>("192.168.1.105"));
  auto orig_p = pbmi.lookup(greater, *to<port>("49320/?"));

  REQUIRE(orig_h);
  CHECK((*orig_h)[1] == 1);
  CHECK((*orig_h)[2] == 1);

  REQUIRE(orig_p);
  CHECK((*orig_p)[1] == 1);
  CHECK((*orig_p)[2] == 0);

  CHECK(rm(dir));
}

TEST("basic actor integrity")
{
  // First spawn the core.
  configuration core_config;
  set_ports(core_config, 2);
  *core_config['v'] = 0;
  *core_config['V'] = 5;
  *core_config['C'] = true;
  *core_config['p'] = "m57-ssl";
  REQUIRE(core_config.verify());

  auto core = spawn<program>(core_config);
  anon_send(core, atom("run"));

  // Wait until the TCP sockets of the core have bound.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Import a single Bro log.
  configuration import_config;
  set_ports(import_config, 2);
  *import_config['v'] = 0;
  *import_config['V'] = 5;
  *import_config['I'] = true;
  *import_config['r'] = m57_day11_18::ssl;
  *import_config["import.max-events-per-chunk"] = 10;
  *import_config["import.max-segment-size"] = 1;
  REQUIRE(import_config.verify());

  // Terminates after import completes.
  auto import = spawn<program>(import_config);

  // Pull down core afterwards.
  import->link_to(core);
  anon_send(import, atom("run"));

  await_all_actors_done();

  // Restart a new core.
  set_ports(core_config, 3);
  *core_config['v'] = 0;
  *core_config['V'] = 5;
  *core_config['C'] = true;
  *core_config['p'] = "m57-conn";
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

  // Test whether the archive has the correct segment.
  self->send(core, atom("archive"));
  self->receive([&](actor archive) { self->send(archive, event_id(100)); });
  self->receive(
      on_arg_match >> [&](segment const& s)
      {
        CHECK(s.base() == 1);
        CHECK(s.events() == 113);

        // Check the last ssl.log entry.
        segment::reader r{&s};
        auto e = r.read(113);
        REQUIRE(e);
        CHECK(get<record>(*e)->at(1) == "XBy0ZlNNWuj");
        CHECK(get<record>(*e)->at(3) == "TLSv10");
      },
      fail);

  // Test whether a manual index lookup succeeds.
  auto pops = to<expr::ast>("id.resp_p == 995/?");
  REQUIRE(pops);

  self->send(core, atom("index"));
  self->receive(
      [&](actor index) { self->send(index, atom("query"), *pops, self); });

  self->receive(
      on(atom("success")) >> [&]
      {
        REQUIRE(true);
      },
      fail);

  self->receive(
      on_arg_match >> [&](bitstream const& hits)
      {
        CHECK(hits.count() == 46);
        CHECK(hits.find_first() == 4);
      },
      fail);

  self->receive(
      on(atom("progress"), arg_match) >> [=](double progress, uint64_t hits)
      {
        CHECK(progress == 1.0);
        CHECK(hits == 46);
      },
      fail);

  // Construct a simple query and verify that the results are correct.
  self->send(core, atom("search"));
  self->receive(
      [&](actor search)
      {
        auto q = "id.resp_p == 995/?";
        self->sync_send(search, atom("query"), self, q).await((
            [&](expr::ast const& ast, actor qry)
            {
              CHECK(ast == *pops);
              self->send(qry, atom("extract"), uint64_t{46});
            },
            fail));
      },
      fail);

  self->receive(
      on(atom("progress"), arg_match) >> [=](double progress, uint64_t hits)
      {
        CHECK(progress == 1.0);
        CHECK(hits == 46);
      },
      fail);

  auto i = 0;
  self->receive_for(i, 46) (
    [&](event const& e)
    {
      // Verify contents from a few random events.
      if (e.id() == 4)
        CHECK(get<record>(e)->at(1) == "KKSlmtmkkxf");

      if (e.id() == 42)
      {
        CHECK(get<record>(e)->at(1) == "7e0gZmKgGS4");
        CHECK(get<record>(e)->at(4) == "TLS_RSA_WITH_RC4_128_MD5");
      }

      // The last event.
      if (e.id() == 103)
        CHECK(get<record>(e)->at(1) == "mXRBhfuUqag");
    },
    fail);

  // A query always sends a "done" atom before terminating.
  self->receive(
      on(atom("done")) >> [&]
      {
        REQUIRE(true);
      },
      fail);

  // Now import another log file.
  set_ports(import_config, 3);
  *import_config['r'] = m57_day11_18::conn;
  import = self->spawn<program, monitored>(import_config);
  anon_send(import, atom("run"));
  self->receive(
      on_arg_match >> [&](down_msg const& d) { CHECK(d.reason == exit::done); },
      fail);

  // Wait for the segment to arrive at the receiver.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  self->send(core, atom("index"));
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

                    auto dir = path{*core_config.get("directory")};
                    auto p = dir / "index" / "m57-conn" / "types" / "conn";
                    REQUIRE(exists(p));
                  },
                  fail);
            },
            fail);
      });

  // Issue a query against both conn and ssl.
  self->send(core, atom("search"));
  self->receive(
      [&](actor search)
      {
        auto q = "id.resp_p == 443/? && ssl.server_name ni \"mozilla\"";
        self->sync_send(search, atom("query"), self, q).await((
            [&](expr::ast const&, actor qry)
            {
              // Extract all results.
              self->send(qry, atom("extract"), uint64_t{0});
              self->monitor(qry);
            },
            fail));
      },
      fail);

  bool done = false;
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

  CHECK(rm(path{*core_config.get("directory")}));
}
