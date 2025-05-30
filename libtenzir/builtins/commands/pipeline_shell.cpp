//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/env.hpp>
#include <tenzir/detail/load_contents.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/exec_pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <unistd.h>

namespace tenzir::plugins::pipeline_shell {

namespace {

class pipeline_shell {};

auto pipeline_shell_command(const invocation& inv, caf::actor_system& sys)
  -> bool {
  return false;
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  auto name() const -> std::string override {
    return "pipeline_shell";
  }

  auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory> override {
    auto pipeline_shell = std::make_unique<command>(
      "pipeline_shell", "internal command",
      command::opts("?tenzir.pipeline_shell")
        .add<std::string>("endpoint", "the endpoint of the node process"));
    auto factory = command::factory{
      {"pipeline_shell",
       [=](const invocation& inv, caf::actor_system& sys) -> caf::message {
         auto success = pipeline_shell_command(inv, sys);
         return caf::make_message(success ? ec::no_error : ec::silent);
       }},
    };
    return {std::move(pipeline_shell), std::move(factory)};
  };
};

} // namespace

} // namespace tenzir::plugins::pipeline_shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pipeline_shell::plugin)
