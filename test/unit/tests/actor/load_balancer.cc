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
  auto lb = spawn<load_balancer>();
  {
    scoped_actor w0;
    scoped_actor w1;
    self->send(lb, atom("add"), atom("worker"), w0);
    self->send(lb, atom("add"), atom("worker"), w1);
    self->send(lb, atom("test"));
    w0->receive(on(atom("test")) >> [&] { CHECK(w0->last_sender() == self); });
    self->send(lb, atom("test"));
    w1->receive(on(atom("test")) >> [&] { CHECK(w1->last_sender() == self); });

    // When one actor is over-loaded, the load-balancer removes it from the
    // round-robin schedule.
    VAST_DEBUG("overloading", w0->address());
    w0->send(lb, flow_control::overload{});

    // The load-balancer skips the overloaded actor and goes to the next one
    // which is underloaded.
    self->send(lb, atom("test"));
    w1->receive(on(atom("test")) >> [&] { CHECK(w1->last_sender() == self); });

    // The load-balancer blocks when all workers are overloaded.
    w1->send(lb, flow_control::overload{});

    // This one will just sit in the load-balancers mailbox.
    self->send(lb, atom("test"));

    // Once a worker is back to normal, the load-balancer resumes its
    // round-robin schedule.
    w1->send(lb, flow_control::underload{});
    w1->receive(on(atom("test")) >> [&] { CHECK(w1->last_sender() == self); });
  }

  self->send_exit(lb, exit::stop);
  self->await_all_other_actors_done();
}
