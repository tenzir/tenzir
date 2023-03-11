//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/logger.hpp>
#include <vast/logical_pipeline.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::exec {

namespace {

caf::expected<void> exec_command(std::span<const std::string> args) {
  if (args.size() != 1)
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("expected exactly one argument, but got {}", args.size()));
  auto pipeline = logical_pipeline::parse(args[0]);
  if (not pipeline)
    return caf::make_error(ec::invalid_argument,
                           fmt::format("failed to parse pipeline: {}",
                                       pipeline.error()));
  auto executor = std::move(*pipeline).make_local_executor();
  // TODO: This command should probably implement signal handling, and check
  // whether a signal was raised in every iteration over the executor. This will
  // likely be easier to implement once we switch to the actor-based
  // asynchronous executor, so we may as well wait until then.
  for (auto&& result : executor) {
    if (not result)
      return result;
  }
  return {};
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
    return "exec";
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto exec = std::make_unique<command>("exec", "execute a pipeline locally",
                                          command::opts("?vast.exec"));
    auto factory = command::factory{
      {"exec",
       [](const invocation& inv, caf::actor_system&) -> caf::message {
         auto result = exec_command(inv.arguments);
         if (not result)
           return caf::make_message(result.error());
         return {};
       }},
    };
    return {std::move(exec), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::exec

VAST_REGISTER_PLUGIN(vast::plugins::exec::plugin)
