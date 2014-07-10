#include <cppa/cppa.hpp>

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

using namespace cppa;
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

TEST("ingestion (all-in-one)")
{
  configuration cfg;
  set_ports(cfg, 0);
  *cfg['v'] = 0;
  *cfg['V'] = 5;
  *cfg['a'] = true;
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

TEST("ingestion (two programs)")
{
  configuration core_config;
  set_ports(core_config, 1);
  *core_config['v'] = 0;
  *core_config['V'] = 5;
  *core_config['a'] = true;
  REQUIRE(core_config.verify());

  auto core = spawn<program>(core_config);
  anon_send(core, atom("run"));

  configuration ingest_config;
  set_ports(ingest_config, 1);
  *ingest_config['v'] = 0;
  *ingest_config['V'] = 5;
  *ingest_config['I'] = true;
  *ingest_config['r'] = m57_day11_18::ssl;
  *ingest_config['p'] = "m57_day11_18";
  REQUIRE(ingest_config.verify());

  // Wait until the TCP sockets of the core have bound.
  ::sleep(1);

  // Terminates after ingestion completes.
  auto import = spawn<program>(ingest_config);
  import->link_to(core);
  anon_send(import, atom("run"));

  await_all_actors_done();
}

// Requires the previous test to run successfully because this one accesses the
// data it has written to disk.
TEST("actor integrity")
{
  configuration cfg;
  set_ports(cfg, 2);
  *cfg['v'] = 0;
  *cfg['V'] = 5;
  *cfg['a'] = true;
  REQUIRE(cfg.verify());

  scoped_actor self;
  auto core = spawn<program>(cfg);
  anon_send(core, atom("run"));

  auto fail = others() >> [&]
  {
    std::cerr << to_string(self->last_dequeued()) << std::endl;
    REQUIRE(false);
  };

  //
  // Archive
  //
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
        CHECK((*e)[1] == "XBy0ZlNNWuj");
        CHECK((*e)[3] == "TLSv10");
      },
      fail);

  //
  // Index (manual querying)
  //
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

  //
  // Query
  //
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
        CHECK(e[1] == "KKSlmtmkkxf");

      if (e.id() == 42)
      {
        CHECK(e[1] == "7e0gZmKgGS4");
        CHECK(e[4] == "TLS_RSA_WITH_RC4_128_MD5");
      }

      // The last event.
      if (e.id() == 103)
        CHECK(e[1] == "mXRBhfuUqag");
    },
    fail);

  self->send_exit(core, exit::done);
  self->await_all_other_actors_done();

  CHECK(rm(path{*cfg.get("directory")}));
}
