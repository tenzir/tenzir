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
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <iostream>

namespace vast::plugins {

/// The EXAMPLE actor interface.
using example_actor = caf::typed_actor<
  // Update the configuration of the EXAMPLE actor.
  caf::reacts_to<atom::config, record>>
  // Conform to the protocol of the PLUGIN ANALYZER actor.
  ::extend_with<system::analyzer_plugin_actor>;

struct example_actor_state {
  uint64_t max_events = std::numeric_limits<uint64_t>::max();
  bool done = false;

  constexpr static inline auto name = "example";
};

example_actor::behavior_type
example(example_actor::stateful_pointer<example_actor_state> self) {
  return {
    [self](atom::config, record config) {
      VAST_TRACE_SCOPE("{} sets configuration {}", self, config);
      for (auto& [key, value] : config) {
        if (key == "max-events") {
          if (auto max_events = caf::get_if<integer>(&value)) {
            VAST_VERBOSE("{} sets max-events to {}", self, *max_events);
            self->state.max_events = *max_events;
          }
        }
      }
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_TRACE_SCOPE("{} hooks into stream {}", self, in);
      return caf::attach_stream_sink(
               self, in,
               // Initialization hook for CAF stream.
               [=](uint64_t& counter) { // reset state
                 VAST_VERBOSE("{} initialized stream", self);
                 counter = 0;
               },
               // Process one stream element at a time.
               [=](uint64_t& counter, table_slice slice) {
                 // If we're already done, discard the remaining table slices in
                 // the stream.
                 if (self->state.done)
                   return;
                 // Accumulate the rows in our table slices.
                 counter += slice.rows();
                 if (counter >= self->state.max_events) {
                   VAST_INFO("{} terminates stream after {} events", self,
                             counter);
                   self->state.done = true;
                   self->quit();
                 }
               },
               // Teardown hook for CAF stream.
               [=](uint64_t&, const caf::error& err) {
                 if (err && err != caf::exit_reason::user_shutdown) {
                   VAST_ERROR("{} finished stream with error: {}", self,
                              render(err));
                   return;
                 }
               })
        .inbound_slot();
    },
    [](atom::status, system::status_verbosity) -> caf::settings {
      // Return an arbitrary settings object here for use in the status command.
      auto result = caf::settings{};
      caf::put(result, "answer", 42);
      return result;
    },
  };
}

/// An example plugin.
class example_plugin final : public virtual analyzer_plugin,
                             public virtual command_plugin {
public:
  /// Loading logic.
  example_plugin() {
    // nop
  }

  /// Teardown logic.
  ~example_plugin() override {
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

  /// Creates an actor that hooks into the input table slice stream.
  /// @param node A pointer to the NODE actor handle.
  system::analyzer_plugin_actor
  make_analyzer(system::node_actor::pointer node) const override {
    // Create a scoped actor for interaction with actors from non-actor
    // contexts.
    auto self = caf::scoped_actor{node->system()};
    // Spawn the actor.
    auto actor = self->spawn(example);
    // Send the configuration to the actor.
    self->send(actor, atom::config_v, config_);
    return actor;
  };

  /// Creates additional commands.
  std::pair<std::unique_ptr<command>, command::factory>
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

} // namespace vast::plugins

VAST_REGISTER_PLUGIN(vast::plugins::example_plugin, 0, 1, 0, 0)
