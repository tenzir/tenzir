#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/actor/partition.h"
#include "vast/actor/task.h"

#include "framework/unit.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("partition")
{
  using bitstream_type = partition::bitstream_type;

  // TODO create a test fixture for this and share with index.
  VAST_INFO("creating test chunks");
  auto t0 = type::record{{"c", type::count{}}, {"s", type::string{}}};
  t0.name("test_record_event");
  auto t1 = type::real{};
  t1.name("test_double_event");
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
  auto p = self->spawn<partition, monitored+priority_aware>(dir, self);
  auto t = self->spawn<task, monitored>(time::snapshot(), chk0.events());
  self->send(p, chk0, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  t = self->spawn<task, monitored>(time::snapshot(), chk1.events());
  self->send(p, chk1, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });

  VAST_INFO("flushing partition through termination");
  self->send_exit(p, exit::done);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == p); });

  VAST_INFO("reloading partition and running a query against it");
  p = self->spawn<partition, monitored+priority_aware>(dir, self);
  auto expr = to<expression>("&time < now && c >= 42 && c < 84");
  REQUIRE(expr);
  self->send(p, *expr, historical_atom::value);
  bool done = false;
  bitstream_type hits;
  self->do_receive(
    [&](expression const& e, bitstream_type const& h, historical_atom)
    {
      CHECK(*expr == e);
      hits |= h;
    },
    [&](done_atom, time::moment, expression const& e)
    {
      CHECK(*expr == e);
      done = true;
    }
  ).until([&] { return done; });
  CHECK(hits.count() == 42);

  VAST_INFO("creating a continuous query");
  expr = to<expression>("s ni \"7\"");  // Must be normalized at this point.
  REQUIRE(expr);
  self->send(p, *expr, continuous_atom::value);

  VAST_INFO("sending another chunk");
  std::vector<event> events(2048);
  for (size_t i = 0; i < events.size(); ++i)
  {
    auto j = chk0.events() + chk1.events() + i;
    events[i] = event::make(record{j, to_string(j)}, t0);
    events[i].id(j);
  }
  t = self->spawn<task, monitored>(time::snapshot(), 2048ull);
  self->send(p, chunk{std::move(events)}, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });

  VAST_INFO("getting continuous hits");
  self->receive(
    [&](expression const& e, bitstream_type const& hits, continuous_atom)
    {
      CHECK(*expr == e);
      // (1524..3571).map { |x| x.to_s }.select { |x| x =~ /7/ }.length == 549
      CHECK(hits.count() == 549);
    });

  VAST_INFO("disabling continuous query and sending another chunk");
  self->send(p, *expr, continuous_atom::value, disable_atom::value);
  auto e = event::make(record{1337u, to_string(1337)}, t0);
  e.id(4711);
  t = self->spawn<task, monitored>(time::snapshot(), 1ull);
  self->send(p, chunk{{std::move(e)}}, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  // Make sure that we didn't get any new hits.
  CHECK(self->mailbox().count() == 0);

  VAST_INFO("cleaning up");
  self->send_exit(p, exit::done);
  self->await_all_other_actors_done();
  rm(dir);
}
