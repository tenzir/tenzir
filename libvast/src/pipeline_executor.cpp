//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline_executor.hpp"

#include "vast/actors.hpp"
#include "vast/atoms.hpp"
#include "vast/connect_to_node.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/diagnostics.hpp"
#include "vast/execution_node.hpp"
#include "vast/pipeline.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/attach_stream_stage.hpp>
#include <caf/downstream.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <iterator>

namespace vast {

void pipeline_executor_state::start_nodes_if_all_spawned() {
  auto untyped_exec_nodes = std::vector<caf::actor>{};
  for (auto node : exec_nodes) {
    if (not node) {
      // Not all spawned yet.
      return;
    }
    untyped_exec_nodes.push_back(caf::actor_cast<caf::actor>(node));
  }
  untyped_exec_nodes.pop_back();
  self
    ->request(exec_nodes.back(), caf::infinite, atom::start_v,
              std::move(untyped_exec_nodes))
    .then(
      [this]() mutable {
        start_rp.deliver();
      },
      [this](caf::error& err) mutable {
        self->quit(err);
        start_rp.deliver(std::move(err));
      });
}

void pipeline_executor_state::spawn_execution_nodes(pipeline pipe) {
  // Spawn pipeline piece by piece.
  auto input_type = operator_type{tag_v<void>};
  auto previous = exec_node_actor{};
  // TODO: Rewrite me.
  bool spawn_remote = false;
  for (auto&& op : std::move(pipe).unwrap()) {
    if (spawn_remote and op->location() == operator_location::local) {
      spawn_remote = false;
    } else if (not spawn_remote
               and op->location() == operator_location::remote) {
      spawn_remote = true;
    }
    auto description = op->to_string();
    if (spawn_remote) {
      if (not node) {
        auto error
          = caf::make_error(ec::invalid_argument,
                            "encountered remote operator, but remote node "
                            "is nullptr");
        start_rp.deliver(error);
        self->quit(error);
        return;
      }
      // TODO: Consider doing this differently.
      auto output_type = op->infer_type(input_type);
      if (not output_type) {
        auto error
          = caf::make_error(ec::invalid_argument, "could not spawn '{}' for {}",
                            description, input_type);
        start_rp.deliver(error);
        self->quit(error);
        return;
      }
      auto index = exec_nodes.size();
      exec_nodes.emplace_back();
      self
        ->request(node, caf::infinite, atom::spawn_v,
                  operator_box{std::move(op)}, input_type,
                  static_cast<receiver_actor<diagnostic>>(self))
        .then(
          [this, index](exec_node_actor& exec_node) {
            // TODO: We should call `quit()` whenever `start()` fails to
            // ensure that this will not be called afterwards (or we check for
            // this case).
            self->monitor(exec_node);
            self->link_to(exec_node);
            exec_nodes[index] = std::move(exec_node);
            start_nodes_if_all_spawned();
          },
          [this](caf::error& err) {
            self->quit(err);
            // TODO: Is this safe?
            start_rp.deliver(err);
          });
      input_type = *output_type;
    } else {
      auto spawn_result
        = spawn_exec_node(self, std::move(op), input_type, node,
                          static_cast<receiver_actor<diagnostic>>(self));
      if (not spawn_result) {
        auto error = caf::make_error(
          ec::logic_error,
          fmt::format("{} failed to spawn execution node "
                      "for operator '{}': {}",
                      *self, description, spawn_result.error()));
        // TODO: Twice the same error?
        self->quit(error);
        start_rp.deliver(error);
        return;
      }
      std::tie(previous, input_type) = std::move(*spawn_result);
      self->monitor(previous);
      self->link_to(previous);
      exec_nodes.push_back(previous);
    }
  }
  if (exec_nodes.empty()) {
    self->quit();
    start_rp.deliver();
    return;
  }
  start_nodes_if_all_spawned();
}

auto pipeline_executor_state::start() -> caf::result<void> {
  if (not this->pipe) {
    return caf::make_error(ec::logic_error,
                           "pipeline exeuctor can only start once");
  }
  auto pipe = *std::exchange(this->pipe, std::nullopt);
  start_rp = self->make_response_promise<void>();
  if (not node) {
    for (const auto& op : pipe.operators()) {
      if (op->location() == operator_location::remote) {
        connect_to_node(self, content(self->system().config()),
                        [this, pipe = std::move(pipe)](
                          caf::expected<node_actor> result) mutable {
                          if (not result) {
                            start_rp.deliver(result.error());
                            self->quit(result.error());
                            return;
                          }
                          node = *result;
                          spawn_execution_nodes(std::move(pipe));
                        });
        return start_rp;
      }
    }
  }
  spawn_execution_nodes(std::move(pipe));
  return start_rp;
}

auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline pipe, std::unique_ptr<diagnostic_handler> diagnostics,
  node_actor node) -> pipeline_executor_actor::behavior_type {
  self->state.self = self;
  self->state.node = std::move(node);
  self->set_down_handler([self](caf::down_msg& msg) {
    VAST_DEBUG("pipeline executor node down: {}; remaining: {}; reason: {}",
               msg.source, self->state.exec_nodes.size() - 1, msg.reason);
    const auto exec_node
      = std::find_if(self->state.exec_nodes.begin(),
                     self->state.exec_nodes.end(), [&](const auto& exec_node) {
                       return exec_node.address() == msg.source;
                     });
    if (exec_node == self->state.exec_nodes.end()) {
      return;
    }
    for (auto it = self->state.exec_nodes.begin(); it != exec_node; ++it) {
      self->demonitor(*it);
      self->send_exit(*it, msg.reason);
    }
    self->state.exec_nodes.erase(self->state.exec_nodes.begin(), exec_node + 1);
    if (msg.reason and msg.reason != caf::exit_reason::unreachable
        and msg.reason != caf::exit_reason::user_shutdown) {
      self->quit(std::move(msg.reason));
    } else if (self->state.exec_nodes.empty()) {
      self->quit();
    }
  });
  auto checked = pipe.check_type<void, void>();
  if (not checked) {
    self->quit(checked.error());
    return pipeline_executor_actor::behavior_type::make_empty_behavior();
  }
  self->state.pipe = std::move(pipe);
  self->state.diagnostics = std::move(diagnostics);
  self->state.allow_unsafe_pipelines
    = caf::get_or(self->system().config(), "tenzir.allow-unsafe-pipelines",
                  self->state.allow_unsafe_pipelines);
  return {
    [self](atom::start) -> caf::result<void> {
      return self->state.start();
    },
    [self](diagnostic& d) -> caf::result<void> {
      VAST_DEBUG("{} received diagnostic: {}", *self, d);
      self->state.diagnostics->emit(std::move(d));
      return {};
    },
  };
}

} // namespace vast
