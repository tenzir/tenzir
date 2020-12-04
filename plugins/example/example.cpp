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

#include "vast/data.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice.hpp"

#include <caf/actor_cast.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <iostream>

using namespace vast;

// clang-format off
using example_actor = caf::typed_actor<
  caf::reacts_to<caf::stream<table_slice>>,
  caf::reacts_to<atom::config, record>
>;
// clang-format on

struct example_actor_state {
  uint64_t max_events = std::numeric_limits<uint64_t>::max();
  bool done = false;

  constexpr static inline auto name = "example";
};

example_actor::behavior_type
spawn_example_actor(example_actor::stateful_pointer<example_actor_state> self) {
  return {
    [=](caf::stream<table_slice> in) {
      VAST_TRACE(self, "hooks into stream", in);
      caf::attach_stream_sink(
        self, in,
        // Initialization hook for CAF stream.
        [=](uint64_t& counter) { // reset state
          VAST_VERBOSE(self, "initialized stream");
          counter = 0;
        },
        // Process one stream element at a time.
        [=](uint64_t& counter, table_slice slice) {
          // If we're already done, discard the remaining table slices in the
          // stream.
          if (self->state.done)
            return;
          // Accumulate the rows in our table slices.
          counter += slice.rows();
          if (counter >= self->state.max_events) {
            VAST_INFO(self, "terminates stream after", counter, "events");
            self->state.done = true;
            self->quit();
          }
        },
        // Teardown hook for CAF stram.
        [=](uint64_t&, const caf::error& err) {
          if (err && err != caf::exit_reason::user_shutdown) {
            VAST_ERROR(self, "finished stream with error:", render(err));
            return;
          }
        });
    },
    [=](atom::config, record config) {
      VAST_TRACE(self, "sets configuration", config);
      for (auto& [key, value] : config) {
        if (key == "max-events") {
          if (auto max_events = caf::get_if<integer>(&value)) {
            VAST_VERBOSE(self, "sets max-events to", *max_events);
            self->state.max_events = *max_events;
          }
        }
      }
    },
  };
}

/// An example plugin.
class example final : public virtual import_plugin,
                      public virtual command_plugin {
public:
  /// Loading logic.
  example() {
    // nop
  }

  /// Teardown logic.
  ~example() override {
    // nop
  }

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  caf::error initialize(data config) override {
    if (auto r = caf::get_if<record>(&config))
      config_ = *r;
    return caf::none;
  }

  /// Returns the unique name of the plugin.
  const char* name() const override {
    return "example";
  }

  /// Creates an actor that hooks into the importer table slice stream.
  /// @param sys The actor system context to spawn the actor in.
  import_stream_sink_actor
  make_import_stream_sink(caf::actor_system& sys) const override {
    // Spawn the actor.
    auto actor = sys.spawn(spawn_example_actor);
    // Send the configuration to the actor.
    if (!config_.empty())
      caf::anon_send(actor, atom::config_v, config_);
    return actor;
  };

  /// Creates additional commands.
  virtual std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto example = std::make_unique<command>(
      "example", "help for the example plugin command",
      "documentation for the example plugin command", command::opts());
    auto example_command
      = [](const invocation&, caf::actor_system&) -> caf::message {
      std::cout << "Hello, world!" << std::endl;
      return caf::none;
    };
    auto factory = command::factory{
      {"example", example_command},
    };
    return {std::move(example), std::move(factory)};
  };

private:
  record config_ = {};
};

VAST_REGISTER_PLUGIN(example, 0, 1, 0, 0)
