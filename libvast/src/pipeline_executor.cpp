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
  VAST_DEBUG("{} successfully spawned {} execution nodes", *self,
             untyped_exec_nodes.size());
  untyped_exec_nodes.pop_back();
  // The exec nodes delegate the `atom::start` message to the preceding exec
  // node. Thus, when we start the last node, all nodes before are started as
  // well, and the request is completed only afterwards.
  self
    ->request(exec_nodes.back(), caf::infinite, atom::start_v,
              std::move(untyped_exec_nodes))
    .then(
      [this]() mutable {
        finish_start();
      },
      [this](caf::error& err) mutable {
        VAST_VERBOSE("{} aborts start because execution node could not be "
                     "started: {}",
                     *self, err);
        abort_start(std::move(err));
      });
}

void pipeline_executor_state::spawn_execution_nodes(pipeline pipe) {
  VAST_DEBUG("{} spawns execution nodes", *self);
  auto input_type = operator_type::make<void>();
  auto previous = exec_node_actor{};
  bool spawn_remote = false;
  // Spawn pipeline piece by piece.
  for (auto&& op : std::move(pipe).unwrap()) {
    // Only switch locations if necessary.
    if (spawn_remote and op->location() == operator_location::local) {
      spawn_remote = false;
    } else if (not spawn_remote
               and op->location() == operator_location::remote) {
      spawn_remote = true;
    }
    auto description = op->to_string();
    if (spawn_remote) {
      VAST_DEBUG("{} spawns {} remotely", *self, description);
      if (not node) {
        abort_start(caf::make_error(
          ec::invalid_argument, "encountered remote operator, but remote node "
                                "is nullptr"));
        return;
      }
      // The node will instantiate the operator for us, but we already need its
      // output type to spawn the following operator.
      auto output_type = op->infer_type(input_type);
      if (not output_type) {
        abort_start(caf::make_error(ec::invalid_argument,
                                    "could not spawn '{}' for {}", description,
                                    input_type));
        return;
      }
      // Allocate an empty handle in the list of exec nodes. When the node actor
      // returns the handle, we set the handle. This is also used to detect when
      // all exec nodes are spawned.
      auto index = exec_nodes.size();
      exec_nodes.emplace_back();
      self
        ->request(node, caf::infinite, atom::spawn_v,
                  operator_box{std::move(op)}, input_type, diagnostics)
        .then(
          [=, this](exec_node_actor& exec_node) {
            VAST_VERBOSE("{} spawned {} remotely", *self, description);
            // TODO: We should call `quit()` whenever `start()` fails to
            // ensure that this will not be called afterwards (or we check for
            // this case).
            self->monitor(exec_node);
            self->link_to(exec_node);
            exec_nodes[index] = std::move(exec_node);
            start_nodes_if_all_spawned();
          },
          [=, this](caf::error& err) {
            VAST_DEBUG("{} failed to spawn {} remotely: {}", *self, description,
                       err);
            abort_start(std::move(err));
          });
      input_type = *output_type;
    } else {
      VAST_DEBUG("{} spawns {} locally", *self, description);
      auto spawn_result
        = spawn_exec_node(self, std::move(op), input_type, node, diagnostics);
      if (not spawn_result) {
        abort_start(add_context(spawn_result.error(),
                                "{} failed to spawn execution node", *self));
        return;
      }
      VAST_DEBUG("{} spawned {} locally", *self, description);
      std::tie(previous, input_type) = std::move(*spawn_result);
      self->monitor(previous);
      self->link_to(previous);
      exec_nodes.push_back(previous);
    }
  }
  if (exec_nodes.empty()) {
    VAST_DEBUG("{} quits because of empty pipeline", *self);
    finish_start();
    self->quit();
    return;
  }
  start_nodes_if_all_spawned();
}

void pipeline_executor_state::abort_start(caf::error reason) {
  if (reason == ec::silent) {
    VAST_DEBUG("{} delivers silent start abort", *self);
    start_rp.deliver(ec::silent);
    self->quit(ec::silent);
    return;
  }
  auto diagnostic = diagnostic::error("{}", reason).done();
  VAST_DEBUG("{} sends diagnostic due to start abort: {}", *self, diagnostic);
  self->request(diagnostics, caf::infinite, std::move(diagnostic))
    .then(
      [this]() {
        // We already delivered the error as a diagnostic.
        VAST_DEBUG("{} delivered diagnostic and shuts down silently", *self);
        start_rp.deliver(ec::silent);
        self->quit(ec::silent);
      },
      [this](caf::error& error) {
        VAST_WARN("{} could not send start diagnostic: {}", *self, error);
        start_rp.deliver(
          add_context(error, "{} could not deliver diagnostic", *self));
        self->quit(ec::silent);
      });
}

void pipeline_executor_state::finish_start() {
  VAST_DEBUG("{} signals successful start", *self);
  start_rp.deliver();
}

auto pipeline_executor_state::start() -> caf::result<void> {
  VAST_DEBUG("{} got start request", *self);
  if (not this->pipe) {
    return caf::make_error(ec::logic_error,
                           "pipeline exeuctor can only start once");
  }
  auto pipe = *std::exchange(this->pipe, std::nullopt);
  start_rp = self->make_response_promise<void>();
  auto checked = pipe.check_type<void, void>();
  if (not checked) {
    VAST_DEBUG("{} failed type check", *self);
    abort_start(checked.error());
    return start_rp;
  }
  if (not node) {
    for (const auto& op : pipe.operators()) {
      if (op->location() == operator_location::remote) {
        VAST_DEBUG("{} connects to node because of remote operators", *self);
        connect_to_node(self, content(self->system().config()),
                        [this, pipe = std::move(pipe)](
                          caf::expected<node_actor> result) mutable {
                          if (not result) {
                            abort_start(std::move(result.error()));
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
  pipeline pipe, receiver_actor<diagnostic> diagnostics, node_actor node)
  -> pipeline_executor_actor::behavior_type {
  VAST_DEBUG("{} was created", *self);
  self->state.self = self;
  self->state.node = std::move(node);
  self->set_down_handler([self](caf::down_msg& msg) {
    VAST_DEBUG(
      "{} received down from execution node {} and has {} remaining: {}", *self,
      msg.source, self->state.exec_nodes.size() - 1, msg.reason);
    const auto exec_node
      = std::find_if(self->state.exec_nodes.begin(),
                     self->state.exec_nodes.end(), [&](const auto& exec_node) {
                       return exec_node.address() == msg.source;
                     });
    if (exec_node == self->state.exec_nodes.end()) {
      return;
    }
    if (auto count = exec_node - self->state.exec_nodes.begin(); count > 0) {
      VAST_VERBOSE("{} kills {} execution nodes without downstream", *self,
                   count);
    }
    for (auto it = self->state.exec_nodes.begin(); it != exec_node; ++it) {
      self->demonitor(*it);
      self->send_exit(*it, caf::exit_reason::kill);
    }
    self->state.exec_nodes.erase(self->state.exec_nodes.begin(), exec_node + 1);
    if (msg.reason and msg.reason != caf::exit_reason::unreachable
        and msg.reason != caf::exit_reason::user_shutdown) {
      self->quit(std::move(msg.reason));
    } else if (self->state.exec_nodes.empty()) {
      self->quit();
    }
  });
  self->state.pipe = std::move(pipe);
  self->state.diagnostics = std::move(diagnostics);
  self->state.allow_unsafe_pipelines
    = caf::get_or(self->system().config(), "tenzir.allow-unsafe-pipelines",
                  self->state.allow_unsafe_pipelines);
  return {
    [self](atom::start) -> caf::result<void> {
      return self->state.start();
    },
  };
}

} // namespace vast
