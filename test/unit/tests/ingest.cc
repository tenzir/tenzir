#include <cppa/cppa.hpp>

#include "vast/configuration.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/program.h"
#include "vast/segment.h"

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
  *cfg['a'] = true;
  *cfg['I'] = true;
  *cfg['r'] = m57_day11_18::ftp;
  *cfg['v'] = 1;

  REQUIRE(cfg.verify());

  anon_send_exit(spawn<program>(cfg), exit::done);
  await_all_actors_done();

  CHECK(rm(path{*cfg.get("directory")}));
}

TEST("ingestion (two programs)")
{
  configuration core_config;
  set_ports(core_config, 1);
  *core_config['a'] = true;
  REQUIRE(core_config.verify());

  auto core = spawn<program>(core_config);

  configuration ingest_config;
  set_ports(ingest_config, 1);
  *ingest_config['I'] = true;
  *ingest_config['r'] = m57_day11_18::ssl;
  REQUIRE(ingest_config.verify());

  // Wait until the TCP sockets of the core have bound.
  ::sleep(1);

  // Terminates after ingestion completes.
  spawn<program>(ingest_config)->link_to(core);
  await_all_actors_done();
}

TEST("actor integrity")
{
  configuration cfg;
  set_ports(cfg, 2);
  *cfg['a'] = true;
  REQUIRE(cfg.verify());

  scoped_actor self;
  auto core = spawn<program>(cfg);

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
      others() >> [&]
      {
        std::cerr << to_string(self->last_dequeued()) << std::endl;
        REQUIRE(false);
      });

  //
  // Index
  //
  auto ast = to<expr::ast>("id.resp_p == 995/?");
  REQUIRE(ast);

  self->send(core, atom("index"));
  self->receive(
      [&](actor index) { self->send(index, atom("query"), *ast, self); });

  // TODO
  self->send_exit(core, exit::done);
  self->await_all_other_actors_done();

  CHECK(rm(path{*cfg.get("directory")}));
}
