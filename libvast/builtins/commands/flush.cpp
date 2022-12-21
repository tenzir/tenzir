//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>

namespace vast::plugins::flush {

namespace {

caf::message flush_command(const invocation& inv, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto node_opt = system::spawn_or_connect_to_node(self, inv.options,
                                                   content(sys.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node
    = std::holds_alternative<system::node_actor>(node_opt)
        ? std::get<system::node_actor>(node_opt)
        : std::get<scope_linked<system::node_actor>>(node_opt).get();
  // Get the index actor.
  auto components
    = system::get_node_components<system::index_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto [index] = std::move(*components);
  // Flush!
  auto result = caf::message{};
  self->request(index, caf::infinite, atom::flush_v)
    .receive(
      []() {
        // nop
      },
      [&](caf::error& err) {
        result = caf::make_message(std::move(err));
      });
  return result;
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  caf::error initialize(data) override {
    return caf::none;
  }

  [[nodiscard]] const char* name() const override {
    return "flush";
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto flush = std::make_unique<command>(
      "flush", "write all currently active partitions to disk",
      command::opts("?vast.flush"));
    auto factory = command::factory{
      {"flush", flush_command},
    };
    return {std::move(flush), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::flush

VAST_REGISTER_PLUGIN(vast::plugins::flush::plugin)
