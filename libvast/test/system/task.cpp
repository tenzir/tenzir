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
#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(task_tests, fixtures::actor_system)

namespace {

behavior worker(event_based_actor* self, const actor& task) {
  return [=](std::string) {
    self->send(task, atom::done_v);
    self->quit();
  };
}

} // namespace <anonymous>

TEST(custom done message) {
  auto t = sys.spawn(task<int>, 42);
  self->send(t, atom::supervisor_v, self);
  self->send_exit(t, exit_reason::user_shutdown);
  self->receive([&](atom::done, int i) { CHECK(i == 42); });
}

TEST(manual task shutdown) {
  auto t = sys.spawn(task<>);
  auto w0 = sys.spawn(worker, t);
  auto w1 = sys.spawn(worker, t);
  self->send(t, atom::supervisor_v, self);
  self->send(t, w0);
  self->send(t, w1);
  self->send(w0, "regular");
  MESSAGE("sending explicit DONE atom");
  self->send(t, atom::done_v, w1->address());
  self->receive([&](atom::done) { /* nop */ });
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
  self->send(t, atom::subscriber_v, self);
  self->send(t, atom::supervisor_v, self);
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
  self->request(t, infinite, atom::progress_v)
    .receive(
      [&](uint64_t remaining, uint64_t total) {
        CHECK(remaining == 3);
        CHECK(total == 3);
      },
      error_handler());
  MESSAGE("asking intermediate task for the current progress");
  self->request(i, infinite, atom::progress_v)
    .receive(
      [&](uint64_t remaining, uint64_t total) {
        CHECK(remaining == 3);
        CHECK(total == 3);
      },
      error_handler());
  MESSAGE("completing intermediate work items");
  self->send(leaf2a, "Go");
  self->send(leaf2b, "make");
  self->send(leaf2c, "money!");
  self->wait_for(i);
  self->receive([&](atom::progress, uint64_t remaining, uint64_t total) {
    CHECK(remaining == 2);
    CHECK(total == 3);
  });
  MESSAGE("completing remaining work items");
  self->send(leaf1a, "Lots");
  self->send(leaf1b, "please!");
  auto n = 1;
  self->receive_for(n,
                    2)([&](atom::progress, uint64_t remaining, uint64_t total) {
    CHECK(remaining == static_cast<uint64_t>(n));
    CHECK(total == 3);
  });
  MESSAGE("checking final notification");
  self->receive([&](atom::done) { CHECK(self->current_sender() == t); });
}

FIXTURE_SCOPE_END()
