#include <caf/all.hpp>
#include "vast/chunk.h"
#include "vast/event.h"
#include "vast/actor/index.h"

#include "framework/unit.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("index")
{
  // TODO create a test fixture for this and share with partition test.
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

  VAST_INFO("sending chunks to index");
  path dir = "vast-test-index";
  scoped_actor self;
  auto i = self->spawn<vast::index, monitored+priority_aware>(dir, 500, 5, 3);
  self->send(i, chk0);
  self->send(i, chk1);

  VAST_INFO("flushing index through termination");
  self->send_exit(i, exit::done);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == i); });

  VAST_INFO("reloading index and running a query against it");
  i = self->spawn<vast::index, monitored+priority_aware>(dir, 500, 5, 3);
  auto expr = to<expression>("c >= 42 && c < 84");
  REQUIRE(expr);
  actor task;
  self->send(i, *expr, self);
  self->receive(
    [&](actor const& t)
    {
      REQUIRE(t != invalid_actor);
      self->monitor(t);
      task = t;
    });

  VAST_INFO("getting results");
  bool done = false;
  default_bitstream hits;
  self->do_receive(
    [&](default_bitstream const& h)
    {
      hits |= h;
    },
    [&](done_atom, time::duration, expression const& e)
    {
      CHECK(*expr == e);
      done = true;
    }
  ).until([&] { return done; });
  self->receive([&](down_msg const& msg) { CHECK(msg.source == task); });
  CHECK(hits.count() == 42);

  VAST_INFO("cleaning up");
  self->send_exit(i, exit::done);
  self->await_all_other_actors_done();
  rm(dir);
}
