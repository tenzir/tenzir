#include <cppa/cppa.hpp>

#include "vast/configuration.h"
#include "vast/file_system.h"
#include "vast/program.h"

#include "framework/unit.h"

#include "test/unit/bro_logs.h"

using namespace cppa;
using namespace vast;

SUITE("actors")

extern uint16_t base_port;

namespace {

configuration cfg;

// Because we're running multiple TCP tests sequentially, we need to give the
// OS a bit of time to release the ports from previous test.
void bump_ports(configuration& config)
{
  *config["tracker.port"] = base_port++;
  *config["receiver.port"] = base_port++;
  *config["archive.port"] = base_port++;
  *config["index.port"] = base_port++;
  *config["search.port"] = base_port++;
}

} // namespace <anonymous>

TEST("ingestion (all-in-one)")
{
  cfg.initialize();

  bump_ports(cfg);
  *cfg['a'] = true;
  *cfg['I'] = true;
  *cfg['r'] = m57_day11_18::smtp;
  *cfg['v'] = 1;

  REQUIRE(cfg.verify());

  anon_send_exit(spawn<program>(cfg), exit::done);
  await_all_actors_done();
  CHECK(rm(path{*cfg.get("directory")}));
}

TEST("ingestion (two processes)")
{
  bump_ports(cfg);
  *cfg['a'] = true;
  *cfg['I'] = false;
  *cfg['r'] = false;

  auto core = spawn<program>(cfg);

  // Wait until the TCP sockets have bound.
  scoped_actor self;
  self->delayed_send(self, std::chrono::seconds(5), "wait");
  self->receive(others() >>
                [&]
                {
                  *cfg['a'] = false;
                  *cfg['I'] = true;
                  *cfg['r'] = m57_day11_18::ftp;
                  auto p = self->spawn<program>(cfg);
                  p->link_to(core);
                });

  self->await_all_other_actors_done();
  CHECK(rm(path{*cfg.get("directory")}));
}
