#include "vast/task_tree.h"

#include "framework/unit.h"

using namespace caf;
using namespace vast;

SUITE("actors")

namespace {

behavior worker(event_based_actor* self, actor const& supervisor)
{
  return
  {
    others() >> [=]
    {
      self->send(supervisor, atom("done"));
      self->quit();
    }
  };
}

} // namespace <anonymous>

// We construct the following task tree in this example:
//
//                   root
//                  / | \
//                 /  |  \
//                I  1a  1b
//               /|\
//              / | \
//            2a 2b 2c
//
TEST("task tree")
{
  scoped_actor self;
  auto t = self->spawn<task_tree, monitored>(self);

  // Subscribe to progress updates and final termination.
  anon_send(t, atom("subscribe"), self);
  anon_send(t, atom("notify"), self);

  auto leaf1a = spawn(worker, t);
  auto leaf1b = spawn(worker, t);
  auto leaf2a = spawn(worker, t);
  auto leaf2b = spawn(worker, t);
  auto leaf2c = spawn(worker, t);

  // Just a dummy node in our example.
  auto intermediate = spawn(
      [&](event_based_actor* self) { self->send(t, atom("done")); });

  anon_send(t, self, leaf1a);
  anon_send(t, self, leaf1b);
  anon_send(t, self, intermediate);
  anon_send(t, intermediate, leaf2a);
  anon_send(t, intermediate, leaf2b);
  anon_send(t, intermediate, leaf2c);

  auto fail = others() >> [&]
  {
    std::cerr << to_string(self->last_dequeued()) << std::endl;
    REQUIRE(false);
  };

  // Ask manually for the current progress.
  self->sync_send(t, atom("progress")).await(behavior{
      [&](uint64_t remaining, uint64_t total)
      {
        CHECK(remaining == 6);
        CHECK(total == 6);
      },
      fail
      });

  // Complete the work.
  anon_send(leaf2a, "Go");
  anon_send(leaf2b, "make");
  anon_send(leaf2c, "money!");
  anon_send(leaf1a, "Lots");
  anon_send(leaf1b, "please!");

  auto i = 0;
  self->receive_for(i, 6) (
      [&](uint64_t remaining, uint64_t total)
      {
        CHECK(remaining == 5 - i);
        CHECK(total == 6);
      },
      fail
      );

  // We wanted to be notified.
  self->receive(
      on(atom("done")) >> [&]{ REQUIRE(true); },
      fail
      );

  // The last message has completed
  self->receive(
      [&](down_msg const&) { REQUIRE(true); },
      fail
      );

  self->await_all_other_actors_done();
}
