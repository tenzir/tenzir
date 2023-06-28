//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline_executor.hpp"

#include "vast/actors.hpp"
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

auto pipeline_executor_state::start() -> caf::result<void> {
  if (not pipe) {
    return caf::make_error(ec::logic_error,
                           "pipeline exeuctor can only run pipeline once");
  }
  // Spawn pipeline piece by piece.
  auto input_type = operator_type{tag_v<void>};
  auto previous = exec_node_actor{};
  // TODO: Rewrite me.
  for (auto&& op : (*std::exchange(pipe, {})).unwrap()) {
    // FIXME: check if op is remote
    auto description = op->to_string();
    auto spawn_result
      = spawn_exec_node(self, std::move(op), input_type, node,
                        static_cast<receiver_actor<diagnostic>>(self));
    if (not spawn_result) {
      auto error = caf::make_error(
        ec::logic_error, fmt::format("{} failed to spawn execution node "
                                     "for operator '{}': {}",
                                     *self, description, spawn_result.error()));
      // TODO: Twice the same error?
      self->quit(error);
      return error;
    }
    std::tie(previous, input_type) = std::move(*spawn_result);
    exec_nodes.push_back(previous);
  }
  if (exec_nodes.empty()) {
    self->quit();
    return {};
  }
  auto untyped_exec_nodes = std::vector<caf::actor>{};
  for (auto node : exec_nodes) {
    untyped_exec_nodes.push_back(caf::actor_cast<caf::actor>(node));
  }
  untyped_exec_nodes.pop_back();
  auto rp = self->make_response_promise<void>();
  self
    ->request(exec_nodes.back(), caf::infinite, atom::start_v,
              std::move(untyped_exec_nodes))
    .then(
      [rp]() mutable {
        rp.deliver();
      },
      [this, rp](caf::error& err) mutable {
        self->quit(err);
        rp.deliver(std::move(err));
      });
  return rp;
}

auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline pipe, std::unique_ptr<diagnostic_handler> diagnostic_handler,
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
    if (msg.reason != caf::error{}
        && msg.reason != caf::exit_reason::unreachable) {
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
  self->state.diagnostic_handler = std::move(diagnostic_handler);
  self->state.allow_unsafe_pipelines
    = caf::get_or(self->system().config(), "vast.allow-unsafe-pipelines",
                  self->state.allow_unsafe_pipelines);
  return {
    [self](atom::start) -> caf::result<void> {
      return self->state.start();
    },
    [self](diagnostic& d) -> caf::result<void> {
      VAST_DEBUG("{} received diagnostic: {}", *self, d);
      self->state.diagnostic_handler->emit(std::move(d));
      return {};
    },
  };
}

} // namespace vast
