//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/connect_to_node.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::flush {

namespace {

caf::message flush_command(const invocation&, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto node_opt = connect_to_node(self, content(sys.config()));
  if (!node_opt)
    return caf::make_message(node_opt.error());
  const auto& node = *node_opt;
  // Get the index actor.
  auto components = get_node_components<importer_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto [importer] = std::move(*components);
  // Flush!
  auto result = caf::message{};
  self->request(importer, caf::infinite, atom::flush_v)
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

  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return caf::none;
  }

  [[nodiscard]] std::string name() const override {
    return "flush";
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto flush = std::make_unique<command>(
      "flush", "write all currently active partitions to disk",
      command::opts("?tenzir.flush"));
    auto factory = command::factory{
      {"flush", flush_command},
    };
    return {std::move(flush), std::move(factory)};
  };
};

} // namespace

} // namespace tenzir::plugins::flush

TENZIR_REGISTER_PLUGIN(tenzir::plugins::flush::plugin)
