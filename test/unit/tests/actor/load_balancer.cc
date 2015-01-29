#include <caf/all.hpp>

#include "vast/actor/load_balancer.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("load balancer")
{
  scoped_actor self;
  auto lb = spawn<load_balancer, priority_aware>();
  {
    // The scoped actors are not priority aware, but we still include the
    // message priorities in the send calls below to illustrate how one should
    // signal over/under-load.
    scoped_actor w0;
    scoped_actor w1;
    self->send(message_priority::high, lb, flow_control::announce{self});
    self->send(lb, atom("add"), atom("worker"), w0);
    self->send(lb, atom("add"), atom("worker"), w1);
    self->send(lb, atom("test"));
    w0->receive(on(atom("test")) >> [&] { CHECK(w0->last_sender() == self); });
    self->send(lb, atom("test"));
    w1->receive(on(atom("test")) >> [&] { CHECK(w1->last_sender() == self); });

    // When one actor is over-loaded, the load-balancer removes it from the
    // round-robin schedule.
    VAST_DEBUG("overloading", w0->address());
    w0->send(message_priority::high, lb, flow_control::overload{});

    // The load-balancer skips the overloaded actor and goes to the next one
    // which is underloaded.
    self->send(lb, atom("test"));
    w1->receive(on(atom("test")) >> [&] { CHECK(w1->last_sender() == self); });

    // Now we overload the other worker so that the entire load-balancer is
    // overloaded.
    VAST_DEBUG("overloading", w1->address());
    w1->send(message_priority::high, lb, flow_control::overload{});
    self->receive(
      [&](flow_control::overload) { CHECK(self->last_sender() == lb); });

    // This one will go to the next actor in the round-robin schedule, which is
    // our first actor. The load-balancer is *not* blocking, but rather hopes
    // that upstream actors adjust their rate accordingly.
    self->send(lb, atom("test"));
    w0->receive(on(atom("test")) >> [&] { CHECK(w0->last_sender() == self); });

    // Once a worker is back to normal, the load-balancer resumes its
    // round-robin schedule.
    VAST_DEBUG("underloading", w1->address());
    w1->send(message_priority::high, lb, flow_control::underload{});
    self->receive(
      [&](flow_control::underload) { CHECK(self->last_sender() == lb); });
  }

  self->send_exit(lb, exit::stop);
  self->await_all_other_actors_done();
}
