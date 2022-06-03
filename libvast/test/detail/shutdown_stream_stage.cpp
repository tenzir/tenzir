//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE shutdown_stream_stage
#include "vast/detail/shutdown_stream_stage.hpp"

#include "vast/atoms.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <caf/actor.hpp>
#include <caf/attach_continuous_stream_source.hpp>
#include <caf/attach_continuous_stream_stage.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

namespace {

// CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::vector<uint32_t>)

struct source_state {
  caf::stream_source_ptr<caf::broadcast_downstream_manager<uint32_t>> source
    = {};

  static inline const char* name = "source";
};

caf::behavior source(caf::stateful_actor<source_state>* self) {
  self->set_exit_handler([=](const caf::exit_msg&) {
    MESSAGE("received exit");
    self->quit();
  });
  return {
    [=](caf::open_atom) {
      MESSAGE("source open");
      self->state.source = caf::attach_continuous_stream_source(
        self,
        // init
        [&](caf::unit_t&) {
          MESSAGE("source init");
        },
        // pull
        [&](caf::unit_t&, caf::downstream<uint32_t>&, size_t) {
          MESSAGE("source pull");
        },
        // done
        [&](const caf::unit_t&) {
          MESSAGE("source done");
          return false;
        },
        // finalize
        [&](caf::unit_t&) {
          MESSAGE("source finalize");
        });
    },
    [=](vast::atom::ping) {
      MESSAGE("source ping");
      for (int i = 0; i < 100; i++)
        self->state.source->out().push(i);
    },
  };
}

struct stage_state {
  bool got_exit = false;
  bool got_ping = false;

  caf::actor sink = {};

  caf::stream_stage_ptr<uint32_t, caf::broadcast_downstream_manager<uint32_t>>
    stage = {};

  static inline const char* name = "stage";
};

caf::behavior stage(caf::stateful_actor<stage_state>* self) {
  self->set_exit_handler([=](const caf::exit_msg&) {
    MESSAGE("received exit");
    self->state.got_exit = true;
    REQUIRE_EQUAL(self->state.got_ping, false);
    self->quit();
  });
  self->state.stage = caf::attach_continuous_stream_stage(
    self, [](caf::unit_t&) {},
    [](caf::unit_t&, caf::downstream<uint32_t>& out, uint32_t x) {
      MESSAGE("stage handle");
      out.push(x);
    },
    [](caf::unit_t&, const caf::error& err) {
      MESSAGE("stage finalize" << err);
    });
  return {
    [=](caf::stream<uint32_t> in) {
      MESSAGE("stage handshake");
      return self->state.stage->add_inbound_path(in);
    },
    [=](caf::actor sink) {
      self->state.sink = std::move(sink);
    },
    [=](vast::atom::ping) {
      MESSAGE("received a ping");
      self->state.got_ping = true;
      REQUIRE_EQUAL(self->state.got_exit, true);
    },
  };
};

struct sink_state {
  caf::actor stage = {};

  static inline const char* name = "sink";
};

caf::behavior sink(caf::stateful_actor<sink_state>* self, caf::actor stage) {
  self->state.stage = std::move(stage);
  return {
    [=](caf::stream<uint32_t> in) {
      MESSAGE("sink handshake");
      // Create a stream manager for implementing a stream sink. Once more, we
      // have to provide three functions: Initializer, Consumer, Finalizer.
      return caf::attach_stream_sink(
        self,
        // Our input source.
        in,
        // Initializer. Here, we store all values we receive. Note that streams
        // are potentially unbound, so this is usually a bad idea outside small
        // examples like this one.
        [](std::vector<uint32_t>&) {
          MESSAGE("sink handle");
          // nop
        },
        // Consumer. Takes individual input elements as `val` and stores them
        // in our history.
        [](std::vector<uint32_t>& xs, uint32_t val) {
          xs.emplace_back(val);
        },
        // Finalizer. Allows us to run cleanup code once the stream terminates.
        [=](std::vector<uint32_t>& xs, const caf::error& err) {
          if (err) {
            MESSAGE("sink aborted with error: " << err);
          } else {
            MESSAGE("sink finalized after receiving: " << xs);
          }
        });
    },
  };
}

using fixture_base = fixtures::deterministic_actor_system;

struct fixture : fixture_base {
  fixture() : fixture_base(VAST_PP_STRINGIFY(SUITE)) {
  }
};
} // namespace

FIXTURE_SCOPE(shutdown_stream_stage_tests, fixture)

TEST(regular messaging) {
  auto stg = self->spawn(stage);
  self->send_exit(stg, caf::exit_reason::unknown);
  self->send(stg, vast::atom::ping_v);
  run();
}

TEST(stream messaging) {
  auto src = self->spawn(source);
  auto stg = self->spawn(stage);
  auto snk = self->spawn(sink, stg);
  self->send(stg, snk);
  auto pipeline = snk * stg * src;
  self->send(pipeline, caf::open_atom_v);
  int runs = 0;
  auto ro = [&] {
    MESSAGE("run: " << runs++);
    run_once();
  };
  self->send(src, vast::atom::ping_v);
  ro();
  ro();
  ro();
  ro();
  ro();
  ro();
  run();
}

FIXTURE_SCOPE_END()
