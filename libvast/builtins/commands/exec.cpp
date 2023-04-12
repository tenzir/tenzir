//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

namespace vast::plugins::exec {

namespace {

auto exec_command(std::span<const std::string> args, record config)
  -> caf::expected<void> {
  if (args.size() != 1)
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("expected exactly one argument, but got {}", args.size()));
  auto pipeline = pipeline::parse(args[0], config);
  if (not pipeline)
    return caf::make_error(ec::invalid_argument,
                           fmt::format("failed to parse pipeline: {}",
                                       pipeline.error()));
  auto executor = make_local_executor(std::move(*pipeline));
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

  auto initialize(const record&, const record& global_config)
    -> caf::error override {
    config_ = global_config;
    return caf::none;
  }

  auto name() const -> std::string override {
    return "exec";
  }

  auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory> override {
    auto exec = std::make_unique<command>("exec", "execute a pipeline locally",
                                          command::opts("?vast.exec"));
    auto factory = command::factory{
      {"exec",
       [=](const invocation& inv, caf::actor_system&) -> caf::message {
         auto result = exec_command(inv.arguments, config_);
         if (not result)
           return caf::make_message(result.error());
         return {};
       }},
    };
    return {std::move(exec), std::move(factory)};
  };

private:
  record config_;
};
} // namespace

} // namespace vast::plugins::exec

VAST_REGISTER_PLUGIN(vast::plugins::exec::plugin)
