#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/actor/partition.h"
#include "vast/actor/task.h"

#define SUITE actors
#include "test.h"
#include "fixtures/chunks.h"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(chunk_scope, fixtures::chunks)

TEST(partition)
{
  using bitstream_type = partition::bitstream_type;

  MESSAGE("sending chunks to partition");
  path dir = "vast-test-partition";
  scoped_actor self;
  auto p = self->spawn<partition, monitored+priority_aware>(dir, self);
  auto t = self->spawn<task, monitored>(time::snapshot(), chunk0.events());
  self->send(p, chunk0, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  t = self->spawn<task, monitored>(time::snapshot(), chunk1.events());
  self->send(p, chunk1, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });

  MESSAGE("flushing partition through termination");
  self->send_exit(p, exit::done);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == p); });

  MESSAGE("reloading partition and running a query against it");
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

  MESSAGE("creating a continuous query");
  expr = to<expression>("s ni \"7\"");  // Must be normalized at this point.
  REQUIRE(expr);
  self->send(p, *expr, continuous_atom::value);

  MESSAGE("sending another chunk");
  std::vector<event> events(2048);
  for (size_t i = 0; i < events.size(); ++i)
  {
    auto j = chunk0.events() + chunk1.events() + i;
    events[i] = event::make(record{j, to_string(j)}, type0);
    events[i].id(j);
  }
  t = self->spawn<task, monitored>(time::snapshot(), 2048ull);
  self->send(p, chunk{std::move(events)}, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });

  MESSAGE("getting continuous hits");
  self->receive(
    [&](expression const& e, bitstream_type const& hits, continuous_atom)
    {
      CHECK(*expr == e);
      // (1524..3571).map { |x| x.to_s }.select { |x| x =~ /7/ }.length == 549
      CHECK(hits.count() == 549);
    });

  MESSAGE("disabling continuous query and sending another chunk");
  self->send(p, *expr, continuous_atom::value, disable_atom::value);
  auto e = event::make(record{1337u, to_string(1337)}, type0);
  e.id(4711);
  t = self->spawn<task, monitored>(time::snapshot(), 1ull);
  self->send(p, chunk{{std::move(e)}}, t);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == t); });
  // Make sure that we didn't get any new hits.
  CHECK(self->mailbox().count() == 0);

  MESSAGE("cleaning up");
  self->send_exit(p, exit::done);
  self->await_all_other_actors_done();
  rm(dir);
}

FIXTURE_SCOPE_END()
