//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/actor_executor.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

#include <caf/scoped_actor.hpp>

namespace vast::plugins::exec {

namespace {

auto exec_command(std::span<const std::string> args, caf::actor_system& sys)
  -> caf::expected<void> {
  if (args.size() != 1)
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("expected exactly one argument, but got {}", args.size()));
  auto pipeline = pipeline::parse(args[0]);
  if (not pipeline)
    return caf::make_error(ec::invalid_argument,
                           fmt::format("failed to parse pipeline: {}",
                                       pipeline.error()));

  caf::scoped_actor self{sys};
  auto executor = self->spawn(pipeline_executor, std::move(*pipeline));
  auto result = caf::expected<void>{};
  // TODO: This command should probably implement signal handling, and check
  // whether a signal was raised in every iteration over the executor. This will
  // likely be easier to implement once we switch to the actor-based
  // asynchronous executor, so we may as well wait until then.
  self->request(executor, caf::infinite, atom::run_v)
    .receive(
      [] {
        // no-op
      },
      [&](caf::error& error) {
        result = std::move(error);
      });
  return result;
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  auto initialize(const record&, const record&) -> caf::error override {
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
       [=](const invocation& inv, caf::actor_system& sys) -> caf::message {
         auto result = exec_command(inv.arguments, sys);
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
