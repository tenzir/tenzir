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

auto pipeline_executor_state::run() -> caf::result<void> {
  if (not pipe) {
    return caf::make_error(ec::logic_error,
                           "pipeline exeuctor can only run pipeline once");
  }
  // Spawn pipeline piece by piece.
  auto input_type = operator_type{tag_v<void>};
  auto previous = exec_node_actor{};
  for (auto&& op : (*std::exchange(pipe, {})).unwrap()) {
    // FIXME: check if op is remote
    auto description = op->to_string();
    auto spawn_result
      = spawn_exec_node(self->system(), std::move(op), input_type,
                        std::move(previous), node,
                        static_cast<receiver_actor<diagnostic>>(self));
    if (not spawn_result) {
      return caf::make_error(
        ec::logic_error, fmt::format("{} failed to spawn execution node "
                                     "for operator '{}': {}",
                                     *self, description, spawn_result.error()));
    }
    std::tie(previous, input_type) = std::move(*spawn_result);
    self->monitor(previous);
    exec_nodes.push_back(previous);
  }
  done_rp = self->make_response_promise<void>();
  return done_rp;
}

auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  const pipeline& pipe, std::unique_ptr<diagnostic_handler> diagnostic_handler,
  node_actor node) -> pipeline_executor_actor::behavior_type {
  self->state.self = self;
  self->state.node = std::move(node);
  self->set_down_handler([self](caf::down_msg& msg) {
    VAST_DEBUG("pipeline executor node down: {}; remaining: {}; reason: {}",
               msg.source, self->state.exec_nodes.size(), msg.reason);
    const auto exec_node
      = std::find_if(self->state.exec_nodes.begin(),
                     self->state.exec_nodes.end(), [&](const auto& exec_node) {
                       return exec_node.address() == msg.source;
                     });
    VAST_ASSERT(exec_node != self->state.exec_nodes.end());
    self->state.exec_nodes.erase(exec_node);
    if (self->state.exec_nodes.empty()) {
      VAST_ASSERT(self->state.done_rp.pending());
      self->state.done_rp.deliver();
    }
  });
  auto optimized = pipe.optimize();
  if (not optimized) {
    self->quit(std::move(optimized.error()));
    return pipeline_executor_actor::behavior_type::make_empty_behavior();
  }
  self->state.pipe = std::move(*optimized);
  self->state.diagnostic_handler = std::move(diagnostic_handler);
  self->state.allow_unsafe_pipelines
    = caf::get_or(self->system().config(), "vast.allow-unsafe-pipelines",
                  self->state.allow_unsafe_pipelines);
  return {
    [self](atom::run) -> caf::result<void> {
      return self->state.run();
    },
    [self](diagnostic& d) -> caf::result<void> {
      VAST_DEBUG("{} received diagnostic: {}", *self, d);
      self->state.diagnostic_handler->emit(std::move(d));
      return {};
    },
  };
}

} // namespace vast
