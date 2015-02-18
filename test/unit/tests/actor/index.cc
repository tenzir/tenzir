#include <caf/all.hpp>
#include "vast/chunk.h"
#include "vast/event.h"
#include "vast/query_options.h"
#include "vast/actor/index.h"

#include "framework/unit.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("index")
{
  using bitstream_type = index::bitstream_type;

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
  self->send(i, *expr, historical, self);
  self->receive(
    [&](actor const& t)
    {
      REQUIRE(t != invalid_actor);
      self->monitor(t);
      task = t;
    });

  VAST_INFO("getting results");
  bool done = false;
  bitstream_type hits;
  self->do_receive(
    [&](bitstream_type const& h)
    {
      hits |= h;
    },
    [&](done_atom, time::extent, expression const& e)
    {
      CHECK(*expr == e);
      done = true;
    }
  ).until([&] { return done; });
  VAST_INFO("completed hit extraction");
  self->receive([&](down_msg const& msg) { CHECK(msg.source == task); });
  CHECK(hits.count() == 42);

  VAST_INFO("creating a continuous query");
  expr = to<expression>("s ni \"7\"");  // Must be normalized at this point.
  REQUIRE(expr);
  self->send(i, *expr, continuous, self);
  self->receive(
    [&](actor const& t)
    {
      REQUIRE(t != invalid_actor);
      self->monitor(t);
      task = t;
    });

  VAST_INFO("sending another chunk and getting continuous hits");
  std::vector<event> events(2048);
  for (size_t i = 0; i < events.size(); ++i)
  {
    auto j = 1524 + i;
    events[i] = event::make(record{j, to_string(j)}, t0);
    events[i].id(j);
  }
  self->send(i, chunk{std::move(events)});
  self->receive([&](bitstream_type const& bs) { CHECK(bs.count() == 549); });

  VAST_INFO("disabling continuous query and sending another chunk");
  self->send(i, *expr, continuous_atom::value, disable_atom::value);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == task); });
  auto e = event::make(record{1337u, to_string(1337)}, t0);
  e.id(4711);
  self->send(i, chunk{{std::move(e)}});
  // Make sure that we didn't get any new hits.
  CHECK(self->mailbox().count() == 0);

  VAST_INFO("cleaning up");
  self->send_exit(i, exit::done);
  self->await_all_other_actors_done();
  rm(dir);
}
