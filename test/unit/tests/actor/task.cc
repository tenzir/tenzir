#include "vast/actor/task.h"

#define SUITE actors
#include "test.h"

using namespace vast;

namespace {

behavior worker(event_based_actor* self, actor const& task) {
  return {
    others() >> [=] {
      self->send(task, done_atom::value);
      self->quit();
    }
  };
}

} // namespace <anonymous>

/* We construct the following task tree hierarchy in this example:
 *
 *                    t
 *                  / | \
 *                 /  |  \
 *                i  1a  1b
 *               /|\
 *              / | \
 *            2a 2b 2c
 *
 * Here, 't' and 'i' represent tasks and the remaining nodes workers.
 */

TEST(task) {
  scoped_actor self;
  auto t = self->spawn(task::make<>);
  self->send(t, subscriber_atom::value, self);
  self->send(t, supervisor_atom::value, self);

  MESSAGE("spawning main workers");
  auto leaf1a = self->spawn(worker, t);
  auto leaf1b = self->spawn(worker, t);
  self->send(t, leaf1a);
  self->send(t, leaf1b);

  MESSAGE("spawning intermediate workers");
  auto i = self->spawn<monitored>(task::make<>);
  self->send(t, i);
  auto leaf2a = self->spawn(worker, i);
  auto leaf2b = self->spawn(worker, i);
  auto leaf2c = self->spawn(worker, i);
  self->send(i, leaf2a);
  self->send(i, leaf2b);
  self->send(i, leaf2c);

  MESSAGE("asking main task for the current progress");
  self->sync_send(t, progress_atom::value).await(
    [&](uint64_t remaining, uint64_t total) {
      CHECK(remaining == 3);
      CHECK(total == 3);
    }
  );
  MESSAGE("asking intermediate task for the current progress");
  self->sync_send(i, progress_atom::value).await(
    [&](uint64_t remaining, uint64_t total) {
      CHECK(remaining == 3);
      CHECK(total == 3);
    }
  );

  MESSAGE("completing intermediate work items");
  self->send(leaf2a, "Go");
  self->send(leaf2b, "make");
  self->send(leaf2c, "money!");
  self->receive([&](down_msg const& msg) { CHECK(msg.source == i); });
  self->receive(
    [&](progress_atom, uint64_t remaining, uint64_t total) {
      CHECK(remaining == 2);
      CHECK(total == 3);
    }
  );

  MESSAGE("completing remaining work items");
  self->send(leaf1a, "Lots");
  self->send(leaf1b, "please!");
  auto n = 1;
  self->receive_for(n, 2) (
    [&](progress_atom, uint64_t remaining, uint64_t total) {
      CHECK(remaining == n);
      CHECK(total == 3);
    }
  );

  MESSAGE("checking final notification");
  self->receive([&](done_atom) { CHECK(self->current_sender() == t); } );

  MESSAGE("customizing an exit message");
  t = spawn(task::make<int>, 42);
  self->send(t, supervisor_atom::value, self);
  self->send_exit(t, exit::kill);
  self->receive([&](done_atom, int i) { CHECK(i == 42); } );

  self->await_all_other_actors_done();
}
