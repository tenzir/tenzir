//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_executor.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/connect_to_node.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/execution_node.hpp"
#include "tenzir/pipeline.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/error.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/policy/select_all.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

namespace tenzir {

void pipeline_executor_state::start_nodes_if_all_spawned() {
  auto untyped_exec_nodes = std::vector<caf::actor>{};
  for (auto node : exec_nodes) {
    if (not node) {
      // Not all spawned yet.
      return;
    }
    untyped_exec_nodes.push_back(caf::actor_cast<caf::actor>(node));
  }
  self->link_to(exec_nodes.back());
  is_started = true;
  TENZIR_DEBUG("{} successfully spawned {} execution nodes", *self,
               untyped_exec_nodes.size());
  untyped_exec_nodes.pop_back();
  // The exec nodes delegate the `atom::start` message to the preceding exec
  // node. Thus, when we start the last node, all nodes before are started as
  // well, and the request is completed only afterwards.
  self->mail(atom::start_v, std::move(untyped_exec_nodes))
    .request(exec_nodes.back(), caf::infinite)
    .then(
      [this]() mutable {
        finish_start();
      },
      [this](const caf::error& err) mutable {
        if (not err) {
          // TODO: Is this even reachable?
          finish_start();
          return;
        }
        abort_start(err);
      });
}

void pipeline_executor_state::spawn_execution_nodes(pipeline pipe) {
  TENZIR_TRACE("{} spawns execution nodes", *self);
  auto input_type = operator_type::make<void>();
  auto previous = exec_node_actor{};
  bool spawn_remote = false;
  // Spawn pipeline piece by piece.
  auto op_index = 0;
  for (auto&& op : std::move(pipe).unwrap()) {
    // Only switch locations if necessary.
    if (spawn_remote and op->location() == operator_location::local) {
      spawn_remote = false;
    } else if (not spawn_remote
               and op->location() == operator_location::remote) {
      spawn_remote = true;
    }
    auto description = fmt::format("{:?}", op);
    if (spawn_remote) {
      TENZIR_TRACE("{} spawns {} remotely", *self, description);
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
        ->mail(atom::spawn_v, operator_box{std::move(op)}, input_type,
               definition, diagnostics, metrics, op_index, is_hidden, run_id)
        .request(node, caf::infinite)
        .then(
          [this, description, index](exec_node_actor& exec_node) {
            TENZIR_VERBOSE("{} spawned {} remotely", *self, description);
            self->monitor(exec_node, [this, source = exec_node->address()](
                                       const caf::error&) {
              const auto exec_node = std::ranges::find(
                exec_nodes, source, &exec_node_actor::address);
              TENZIR_ASSERT(exec_node != exec_nodes.end());
              exec_nodes.erase(exec_node);
            });
            exec_nodes[index] = std::move(exec_node);
            start_nodes_if_all_spawned();
          },
          [this, description](const caf::error& err) {
            abort_start(diagnostic::error(err)
                          .note("failed to spawn {} remotely", description)
                          .to_error());
          });
      input_type = *output_type;
    } else {
      TENZIR_TRACE("{} spawns {} locally", *self, description);
      auto spawn_result
        = spawn_exec_node(self, std::move(op), input_type, definition, node,
                          diagnostics, metrics, op_index, has_terminal,
                          is_hidden, run_id);
      if (not spawn_result) {
        abort_start(diagnostic::error(spawn_result.error())
                      .note("failed to spawn {} locally", description)
                      .to_error());
        return;
      }
      TENZIR_TRACE("{} spawned {} locally", *self, description);
      std::tie(previous, input_type) = std::move(*spawn_result);
      self->monitor(previous, [this, source
                                     = previous->address()](const caf::error&) {
        const auto exec_node
          = std::ranges::find(exec_nodes, source, &exec_node_actor::address);
        TENZIR_ASSERT(exec_node != exec_nodes.end());
        exec_nodes.erase(exec_node);
      });
      exec_nodes.push_back(previous);
    }
    ++op_index;
  }
  if (exec_nodes.empty()) {
    TENZIR_DEBUG("{} quits because of empty pipeline", *self);
    finish_start();
    self->quit();
    return;
  }
  start_nodes_if_all_spawned();
}

void pipeline_executor_state::abort_start(diagnostic reason) {
  TENZIR_DEBUG("{} sends diagnostic due to start abort: {:?}", *self, reason);
  auto err = caf::make_error(ec::diagnostic, std::move(reason));
  start_rp.deliver(std::move(err));
  self->quit(ec::silent);
}

void pipeline_executor_state::abort_start(caf::error reason) {
  TENZIR_ASSERT(reason);
  if (reason == ec::silent) {
    TENZIR_DEBUG("{} delivers silent start abort", *self);
    start_rp.deliver(ec::silent);
    self->quit(ec::silent);
    return;
  }
  abort_start(diagnostic::error(std::move(reason)).done());
}

void pipeline_executor_state::finish_start() {
  TENZIR_TRACE("{} signals successful start", *self);
  start_rp.deliver();
}

auto pipeline_executor_state::start() -> caf::result<void> {
  TENZIR_TRACE("{} got start request", *self);
  if (not this->pipe) {
    return caf::make_error(ec::logic_error,
                           "pipeline exeuctor can only start once");
  }
  auto pipe = *std::exchange(this->pipe, std::nullopt);
  start_rp = self->make_response_promise<void>();
  auto output = pipe.infer_type<void>();
  if (not output) {
    TENZIR_DEBUG("{} failed type inference", *self);
    abort_start(diagnostic::error(output.error())
                  .note("failed type inference")
                  .to_error());
    return start_rp;
  }
  if (not output->is<void>()) {
    TENZIR_DEBUG("{} fails because pipeline ends with {}", *self,
                 operator_type_name(*output));
    auto ops = pipe.operators();
    auto suffix = std::string{};
    if (not ops.empty()) {
      suffix = fmt::format(" instead of `{}`", ops.back()->name());
    }
    abort_start(
      diagnostic::error("expected pipeline to end with a sink{}", suffix)
        .docs("https://docs.tenzir.com/reference/operators")
        .done());
    return start_rp;
  }
  if (not node) {
    for (const auto& op : pipe.operators()) {
      if (op->location() == operator_location::remote) {
        TENZIR_TRACE("{} connects to node because of remote operators", *self);
        connect_to_node(self, [this, pipe = std::move(pipe)](
                                caf::expected<node_actor> result) mutable {
          if (not result) {
            abort_start(diagnostic::error(result.error())
                          .note("failed to connect to node")
                          .to_error());
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

auto pipeline_executor_state::pause() -> caf::result<void> {
  if (start_rp.pending()) {
    return caf::make_error(ec::logic_error,
                           "cannot pause a pipeline before it was started");
  }
  TENZIR_ASSERT(not exec_nodes.empty());
  auto rp = self->make_response_promise<void>();
  self
    ->fan_out_request<caf::policy::select_all>(exec_nodes, caf::infinite,
                                               atom::pause_v)
    .then(
      [rp]() mutable {
        rp.deliver();
      },
      [rp](const caf::error& err) mutable {
        rp.deliver(
          diagnostic::error(err).note("failed to pause exec-node").to_error());
      });
  return rp;
}

auto pipeline_executor_state::resume() -> caf::result<void> {
  TENZIR_ASSERT(not exec_nodes.empty());
  auto rp = self->make_response_promise<void>();
  self
    ->fan_out_request<caf::policy::select_all>(exec_nodes, caf::infinite,
                                               atom::resume_v)
    .then(
      [rp]() mutable {
        rp.deliver();
      },
      [rp](const caf::error& err) mutable {
        rp.deliver(
          diagnostic::error(err).note("failed to resume exec-node").to_error());
      });
  return rp;
}

auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline pipe, std::string definition, receiver_actor<diagnostic> diagnostics,
  metrics_receiver_actor metrics, node_actor node, bool has_terminal,
  bool is_hidden) -> pipeline_executor_actor::behavior_type {
  TENZIR_TRACE("{} was created", *self);
  self->attach_functor([self] {
    TENZIR_TRACE("{} was destroyed", *self);
  });
  self->state().self = self;
  self->state().node = std::move(node);
  self->state().definition = std::move(definition);
  self->state().pipe = std::move(pipe);
  self->state().diagnostics = std::move(diagnostics);
  self->state().metrics = std::move(metrics);
  self->state().no_location_overrides = caf::get_or(
    self->system().config(), "tenzir.no-location-overrides", false);
  self->state().has_terminal = has_terminal;
  self->state().is_hidden = is_hidden;
  return {
    [self](atom::start) -> caf::result<void> {
      return self->state().start();
    },
    [self](atom::pause) -> caf::result<void> {
      return self->state().pause();
    },
    [self](atom::resume) -> caf::result<void> {
      return self->state().resume();
    },
    [self](caf::exit_msg msg) {
      if (self->state().is_started) {
        // If we get an exit message, then it's either because the last
        // execution node died, or because the pipeline manager sent us an exit
        // message. In either case we just wan to shut down all execution nodes.
        for (const auto& exec_node : self->state().exec_nodes) {
          TENZIR_ASSERT(exec_node);
          self->send_exit(exec_node, msg.reason);
        }
        self->quit(std::move(msg.reason));
        return;
      }
      if (msg.reason) {
        self->quit(std::move(msg.reason));
      }
    },
  };
}

} // namespace tenzir
