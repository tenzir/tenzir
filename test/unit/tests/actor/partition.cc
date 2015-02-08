#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/actor/partition.h"

#include "framework/unit.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("partition")
{
  // TODO create a test fixture for this and share with index.
  VAST_INFO("creating test chunks");
  auto t0 = type::record{{"c", type::count{}}, {"s", type::string{}}};
  t0.name("test-record-event");
  auto t1 = type::real{};
  t1.name("test-double-event");
  chunk chk0;
  chunk::writer w0{chk0};
  for (size_t i = 0; i < 1024; ++i)
  {
    auto e = event::make(record{i, to_string(i)}, t0);
    e.id(i);
    e.timestamp(time::now());
    REQUIRE(w0.write(e));
  }
  w0.flush();
  chunk chk1;
  chunk::writer w1{chk1};
  for (size_t i = chk0.events(); i < chk0.events() + 500; ++i)
  {
    auto e = event::make(4.2 + i, t1);
    e.id(i);
    e.timestamp(time::now());
    REQUIRE(w1.write(e));
  }
  w1.flush();

  VAST_INFO("sending chunks to partition");
  path dir = "vast-test-partition";
  scoped_actor self;
  auto p = self->spawn<partition, monitored+priority_aware>(dir);
  self->send(p, chk0);
  self->send(p, chk1);

  VAST_INFO("flushing partition through termination");
  self->send_exit(p, exit::done);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == p); });

  VAST_INFO("reloading partition and running a query against it");
  p = self->spawn<partition, monitored+priority_aware>(dir);
  auto expr = to<expression>("&time < now && c >= 42 && c < 84");
  REQUIRE(expr);
  self->send(p, *expr, self);
  bool done = false;
  default_bitstream hits;
  self->do_receive(
    [&](expression const& e, default_bitstream const& h)
    {
      CHECK(*expr == e);
      hits |= h;
    },
    [&](done_atom, time::duration, expression const& e)
    {
      CHECK(*expr == e);
      done = true;
    }
  ).until([&] { return done; });
  CHECK(hits.count() == 42);

  VAST_INFO("cleaning up");
  self->send_exit(p, exit::done);
  self->await_all_other_actors_done();
  rm(dir);
}
