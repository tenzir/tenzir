#include "vast/event.h"
#include "vast/query_options.h"
#include "vast/actor/index.h"
#include "vast/concept/printable/vast/expression.h"

#define SUITE actors
#include "test.h"
#include "fixtures/events.h"

using namespace vast;

FIXTURE_SCOPE(fixture_scope, fixtures::simple_events)

TEST(index) {
  using bitstream_type = index::bitstream_type;

  MESSAGE("sending events to index");
  path dir = "vast-test-index";
  scoped_actor self;
  auto idx = self->spawn<vast::index, priority_aware>(dir, 500, 2, 3);
  self->send(idx, events0);
  self->send(idx, events1);

  MESSAGE("flushing index through termination");
  self->send_exit(idx, exit::done);
  self->await_all_other_actors_done();

  MESSAGE("reloading index and running a query against it");
  idx = self->spawn<vast::index, priority_aware>(dir, 500, 2, 3);
  auto expr = vast::detail::to_expression("c >= 42 && c < 84");
  REQUIRE(expr);
  actor task;
  self->send(idx, *expr, historical, self);
  self->receive(
    [&](actor const& t) {
      REQUIRE(t != invalid_actor);
      self->monitor(t);
      task = t;
    });

  MESSAGE("getting results");
  bool done = false;
  bitstream_type hits;
  self->do_receive(
    [&](bitstream_type const& h) {
      hits |= h;
    },
    [&](done_atom, time::moment, time::extent, expression const& e) {
      CHECK(*expr == e);
      done = true;
    }
  ).until([&] { return done; });
  MESSAGE("completed hit extraction");
  self->receive([&](down_msg const& msg) { CHECK(msg.source == task); });
  CHECK(hits.count() == 42);

  MESSAGE("creating a continuous query");
  // The expression must have already been normalized as it hits the index.
  expr = vast::detail::to_expression("s ni \"7\"");
  REQUIRE(expr);
  self->send(idx, *expr, continuous, self);
  self->receive(
    [&](actor const& t) {
      REQUIRE(t != invalid_actor);
      self->monitor(t);
      task = t;
    });

  MESSAGE("sending another event batch and getting continuous hits");
  self->send(idx, events);
  self->receive([&](bitstream_type const& bs) { CHECK(bs.count() == 95); });

  MESSAGE("disabling continuous query and sending another event");
  self->send(idx, *expr, continuous_atom::value, disable_atom::value);
  self->receive([&](down_msg const& msg) { CHECK(msg.source == task); });
  auto e = event::make(record{1337u, std::to_string(1337)}, type0);
  e.id(4711);
  self->send(idx, std::vector<event>{std::move(e)});
  // Make sure that we didn't get any new hits.
  CHECK(self->mailbox().count() == 0);

  MESSAGE("cleaning up");
  self->send_exit(idx, exit::done);
  self->await_all_other_actors_done();
  rm(dir);
}

FIXTURE_SCOPE_END()
