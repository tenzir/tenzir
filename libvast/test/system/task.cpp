/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include <caf/all.hpp>

#include "vast/system/task.hpp"

#define SUITE system
#include "test.hpp"
#include "fixtures/actor_system.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(task_tests, fixtures::actor_system)

namespace {

behavior worker(event_based_actor* self, actor const& task) {
  return [=](std::string) {
    self->send(task, done_atom::value);
    self->quit();
  };
}

} // namespace <anonymous>

TEST(custom done message) {
  auto t = system.spawn(task<int>, 42);
  self->send(t, supervisor_atom::value, self);
  self->send_exit(t, exit_reason::user_shutdown);
  self->receive([&](done_atom, int i) { CHECK(i == 42); } );
}

TEST(manual task shutdown) {
  auto t = system.spawn(task<>);
  auto w0 = system.spawn(worker, t);
  auto w1 = system.spawn(worker, t);
  self->send(t, supervisor_atom::value, self);
  self->send(t, w0);
  self->send(t, w1);
  self->send(w0, "regular");
  MESSAGE("sending explicit DONE atom");
  self->send(t, done_atom::value, w1->address());
  self->receive([&](done_atom) {/* nop */});
  self->send_exit(w1, exit_reason::user_shutdown);
}

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
TEST(hierarchical task) {
  MESSAGE("spawning task");
  auto t = self->spawn(task<>);
  self->send(t, subscriber_atom::value, self);
  self->send(t, supervisor_atom::value, self);
  MESSAGE("spawning main workers");
  auto leaf1a = self->spawn(worker, t);
  auto leaf1b = self->spawn(worker, t);
  self->send(t, leaf1a);
  self->send(t, leaf1b);
  MESSAGE("spawning intermediate workers");
  auto i = self->spawn(task<>);
  self->send(t, i);
  auto leaf2a = self->spawn(worker, i);
  auto leaf2b = self->spawn(worker, i);
  auto leaf2c = self->spawn(worker, i);
  self->send(i, leaf2a);
  self->send(i, leaf2b);
  self->send(i, leaf2c);
  MESSAGE("asking main task for the current progress");
  self->request(t, infinite, progress_atom::value).receive(
    [&](uint64_t remaining, uint64_t total) {
      CHECK(remaining == 3);
      CHECK(total == 3);
    },
    error_handler()
  );
  MESSAGE("asking intermediate task for the current progress");
  self->request(i, infinite, progress_atom::value).receive(
    [&](uint64_t remaining, uint64_t total) {
      CHECK(remaining == 3);
      CHECK(total == 3);
    },
    error_handler()
  );
  MESSAGE("completing intermediate work items");
  self->send(leaf2a, "Go");
  self->send(leaf2b, "make");
  self->send(leaf2c, "money!");
  self->wait_for(i);
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
      CHECK(remaining == static_cast<uint64_t>(n));
      CHECK(total == 3);
    }
  );
  MESSAGE("checking final notification");
  self->receive([&](done_atom) { CHECK(self->current_sender() == t); } );
}

FIXTURE_SCOPE_END()
