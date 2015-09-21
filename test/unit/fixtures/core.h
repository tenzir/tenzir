#ifndef FIXTURES_CORE_H
#define FIXTURES_CORE_H

#include <vector>

#include "vast/actor/node.h"

using namespace vast;

namespace fixtures {

struct core {
  core() {
    self->on_sync_failure([&] {
      VAST_ERROR("got unexpected reply:", to_string(self->current_message()));
      self->quit(exit::error);
    });
    if (exists(dir)) {
      MESSAGE("removing existing directory");
      if (! rm(dir))
        FAIL("failed to remove test directory: " << std::strerror(errno));
    }
  }

  ~core() {
    self->await_all_other_actors_done();
    if (exists(dir)) {
      MESSAGE("removing created directory");
      if (! rm(dir))
        FAIL("failed to remove test directory: " << std::strerror(errno));
    }
  }

  actor make_core() {
    auto n = self->spawn(node::make, node_name, dir);
    auto hdl = self->sync_send(n, "spawn", "core",
                               "--archive-segments=1",
                               "--index-events=10");
    hdl.await(
      [](ok_atom) {},
      [&](error const& e) {
        FAIL(e);
      }
    );
    return n;
  }

  void stop_core(actor const& n) {
    MESSAGE("stopping node");
    self->monitor(n);
    self->send_exit(n, exit::stop);
    self->receive(
      [&](down_msg const& msg) {
        CHECK(msg.source == n->address());
      }
    );
  }

  template <typename... Args>
  void run_source(actor const& n, Args&&... args) {
    std::vector<message> msgs = {
      make_message("spawn", "source", std::forward<Args>(args)...),
      make_message("connect", "source", "importer"),
      make_message("send", "source", "run")
    };
    for (auto& msg : msgs)
      self->sync_send(n, msg).await(
        [&](error const& e) {
          FAIL(e);
        },
        others >> [] {
          // Everyting except error is a valid return value.
        }
      );
    MESSAGE("monitoring source");
    self->sync_send(n, store_atom::value, get_atom::value, actor_atom::value,
                    "source").await(
      [&](actor const& a, std::string const& fqn, std::string const& type) {
        CHECK(a != invalid_actor);
        CHECK(fqn == "source@" + node_name);
        CHECK(type == "source");
        self->monitor(a);
        MESSAGE("waiting for source to terminate");
        self->receive(
          [&](down_msg const& dm) { CHECK(dm.reason == exit::done); }
        );
      },
      [](vast::none) {
        // source has already terminated.
      }
    );
  }

  std::string const node_name = "test-node";
  path dir = "vast-unit-test";
  scoped_actor self;
};

} // namespace fixtures

#endif
