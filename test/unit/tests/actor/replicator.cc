#include <caf/all.hpp>

#include "vast/actor/replicator.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("replicator")
{
  scoped_actor self;
  auto r = spawn<replicator>();
  {
    scoped_actor w0;
    scoped_actor w1;
    self->send(r, atom("add"), atom("worker"), w0);
    self->send(r, atom("add"), atom("worker"), w1);
    self->send(r, atom("test"));
    w0->receive(on(atom("test")) >> [&] { CHECK(w0->last_sender() == self); });
    w1->receive(on(atom("test")) >> [&] { CHECK(w1->last_sender() == self); });
  }

  self->send_exit(r, exit::stop);
  self->await_all_other_actors_done();
}
