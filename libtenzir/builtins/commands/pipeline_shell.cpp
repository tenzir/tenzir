//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>
#include <tenzir/connect_to_node.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::pipeline_shell {

namespace {

class pipeline_shell {
public:
  pipeline_shell(pipeline_shell_actor::pointer self) : self_{self} {
  }

  auto make_behavior() -> pipeline_shell_actor::behavior_type {
    return {
      [this](atom::spawn, operator_box& box, operator_type input_type,
             std::string definition,
             const receiver_actor<diagnostic>& diagnostic_handler,
             const metrics_receiver_actor& metrics_receiver, int32_t index,
             bool is_hidden, uuid run_id) -> caf::result<exec_node_actor> {},
    };
  }

  pipeline_shell_actor::pointer self_;
};

auto pipeline_shell_command(const invocation& inv, caf::actor_system& sys)
  -> caf::message {
  if (inv.arguments.size() != 2) {
    return caf::make_message(ec::silent);
  }
  TENZIR_INFO("Hi from the pipeline shell");
  caf::scoped_actor self{sys};
  auto port = inv.arguments[0];
  auto identifier = inv.arguments[1];
  auto node_opt = connect_to_node(self);
  if (not node_opt) {
    return caf::make_message(std::move(node_opt.error()));
  }
  auto result = caf::expected<caf::actor>{caf::error{}};
  const auto node = std::move(*node_opt);
  auto shell = self->spawn(caf::actor_from_state<pipeline_shell>);
  self->mail(atom::connect_v, atom::shell_v, identifier, shell).send(node);
  self->wait_for(shell);
  return caf::make_message(ec::no_error);
}

class plugin final : public command_plugin {
public:
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
      {"pipeline_shell", pipeline_shell_command}
    };
    return {std::move(pipeline_shell), std::move(factory)};
  };
};

} // namespace

} // namespace tenzir::plugins::pipeline_shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pipeline_shell::plugin)
