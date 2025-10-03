//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/connect_to_node.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/execution_node.hpp>
#include <tenzir/plugin.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/scoped_actor.hpp>

namespace tenzir::plugins::pipeline_shell {

namespace {

class pipeline_shell {
public:
  pipeline_shell(pipeline_shell_actor::pointer self, node_actor node)
    : self_{self}, node_{std::move(node)} {
  }

  auto make_behavior() -> pipeline_shell_actor::behavior_type {
    return {
      [this](atom::spawn, operator_box box, operator_type input_type,
             std::string definition, std::string pipeline_id,
             const receiver_actor<diagnostic>& diagnostic_handler,
             const metrics_receiver_actor& metrics_receiver, int32_t index,
             bool is_hidden, uuid run_id) -> caf::result<exec_node_actor> {
        return spawn_exec_node(std::move(box), input_type,
                               std::move(definition), std::move(pipeline_id),
                               diagnostic_handler, metrics_receiver, index,
                               is_hidden, run_id);
      },
    };
  }

  auto spawn_exec_node(operator_box box, operator_type input_type,
                       std::string definition, std::string pipeline_id,
                       const receiver_actor<diagnostic>& diagnostic_handler,
                       const metrics_receiver_actor& metrics_receiver,
                       int32_t index, bool is_hidden, uuid run_id)
    -> caf::result<exec_node_actor> {
    TENZIR_ASSERT(box);
    auto op = std::move(box).unwrap();
    auto spawn_result
      = tenzir::spawn_exec_node(self_, std::move(op), input_type,
                                std::move(definition), std::move(pipeline_id),
                                node_, diagnostic_handler, metrics_receiver,
                                index, false, is_hidden, run_id);
    if (not spawn_result) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} failed to spawn execution node "
                                         "for operator '{:?}': {}",
                                         *self_, op, spawn_result.error()));
    }
    self_->monitor(spawn_result->first,
                   [this, source
                          = spawn_result->first->address()](const caf::error&) {
                     const auto num_erased = monitored_exec_nodes.erase(source);
                     TENZIR_ASSERT(num_erased == 1);
                   });
    monitored_exec_nodes.insert(spawn_result->first->address());
    return spawn_result->first;
  }

  /// Weak handles to remotely spawned and monitored exec nodes for cleanup on
  /// node shutdown.
  std::unordered_set<caf::actor_addr> monitored_exec_nodes;

  pipeline_shell_actor::pointer self_;
  node_actor node_;
};

auto pipeline_shell_command(const invocation& inv, caf::actor_system& sys)
  -> caf::message {
  if (inv.arguments.size() != 2) {
    return caf::make_message(ec::silent);
  }
  auto self = caf::scoped_actor{sys};
  auto endpoint = to<tenzir::endpoint>(inv.arguments[0]);
  TENZIR_ASSERT(endpoint);
  auto identifier
    = static_cast<std::uint32_t>(std::stoul(inv.arguments[1], nullptr));
  auto node_opt = connect_to_node(self, *endpoint, caf::infinite, std::nullopt,
                                  /*internal_connection=*/true);
  if (not node_opt) {
    return caf::make_message(std::move(node_opt.error()));
  }
  auto result = caf::expected<caf::actor>{caf::error{}};
  const auto node = std::move(*node_opt);
  auto shell = self->spawn(caf::actor_from_state<pipeline_shell>, node);
  auto error = caf::error{ec::no_error};
  self->mail(atom::connect_v, atom::shell_v, identifier, shell)
    .request(node, caf::infinite)
    .receive([]() {},
             [&](caf::error err) {
               error = std::move(err);
             });
  if (error) {
    return caf::make_message(error);
  }
  self->monitor(node);
  self->monitor(shell);
  self->receive([&](caf::down_msg& msg) {
    if (msg.source == node) {
      TENZIR_DEBUG("pipeline_shell_command received DOWN from node");
      self->send_exit(shell, msg.reason);
    }
    if (msg.source == shell) {
      TENZIR_DEBUG("pipeline_shell_command received DOWN from shell");
    }
    if (msg.reason != caf::exit_reason::user_shutdown) {
      error = std::move(msg.reason);
    }
  });
  return caf::make_message(error);
}

class plugin final : public command_plugin {
public:
  auto name() const -> std::string override {
    return "pipeline_shell";
  }

  auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory> override {
    auto pipeline_shell
      = std::make_unique<command>("pipeline_shell", "internal command",
                                  command::opts("?tenzir.pipeline_shell"));
    auto factory = command::factory{{"pipeline_shell", pipeline_shell_command}};
    return {std::move(pipeline_shell), std::move(factory)};
  };
};

} // namespace

} // namespace tenzir::plugins::pipeline_shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pipeline_shell::plugin)
