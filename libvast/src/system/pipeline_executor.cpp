//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline_executor.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/execution_node.hpp"
#include "vast/pipeline.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/connect_to_node.hpp"

#include <caf/attach_stream_sink.hpp>
#include <caf/attach_stream_source.hpp>
#include <caf/attach_stream_stage.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <iterator>

namespace vast {

namespace {

template <typename T>
auto flatten(std::vector<std::vector<T>> vecs) -> std::vector<T> {
  auto result = std::vector<T>{};
  for (auto& vec : vecs) {
    result.insert(result.end(), std::move_iterator{vec.begin()},
                  std::move_iterator{vec.end()});
  }
  return result;
}

} // namespace

void pipeline_executor_state::spawn_execution_nodes(
  system::node_actor remote, std::vector<operator_ptr> ops) {
  VAST_DEBUG("spawning execution nodes (remote = {})", remote);
  hosts.reserve(ops.size());
  for (auto it = ops.begin(); it != ops.end(); ++it) {
    switch ((*it)->location()) {
      case operator_location::local:
      case operator_location::anywhere: {
        // Spawn and collect execution nodes until the first remote operator.
        auto& v = hosts.emplace_back();
        while (true) {
          auto description = (*it)->to_string();
          if ((*it)->detached()) {
            v.push_back(caf::actor_cast<caf::actor>(
              self->spawn<caf::monitored + caf::detached>(
                execution_node, std::move(*it), system::node_actor{})));
          } else {
            v.push_back(caf::actor_cast<caf::actor>(self->spawn<caf::monitored>(
              execution_node, std::move(*it), system::node_actor{})));
          }
          node_descriptions.emplace(v.back().address(), std::move(description));
          nodes_alive += 1;
          ++it;
          if (it == ops.end()
              || (*it)->location() == operator_location::remote) {
            break;
          }
        }
        --it;
        break;
      }
      case operator_location::remote: {
        // Spawn and collect execution nodes until the first local operator.
        auto begin = it;
        while (++it != ops.end()) {
          if ((*it)->location() == operator_location::local) {
            break;
          }
        }
        auto end = it;
        --it;
        VAST_ASSERT(remote);
        auto subpipe
          = pipeline{{std::move_iterator{begin}, std::move_iterator{end}}};
        // Allocate a slot in `hosts`, saving its index.
        auto host = hosts.size();
        hosts.emplace_back();
        // We keep track of the remote spawning calls in order to continue
        // only after remoting spawning is complete.
        remote_spawn_count += 1;
        self->request(remote, caf::infinite, atom::spawn_v, std::move(subpipe))
          .then(
            [=, this](
              std::vector<std::pair<system::execution_node_actor, std::string>>&
                execution_nodes) mutable {
              // The number of execution nodes should match the number of
              // operators.
              auto expected_count = detail::narrow<size_t>(end - begin);
              if (execution_nodes.size() != expected_count) {
                VAST_WARN("expected {} execution nodes but got {}",
                          expected_count, execution_nodes.size());
              }
              // Insert the handles into `hosts`.
              VAST_ASSERT(hosts[host].empty());
              hosts[host].reserve(execution_nodes.size());
              for (auto& [node, description] : execution_nodes) {
                self->monitor(node);
                nodes_alive += 1;
                node_descriptions.emplace(node.address(),
                                          std::move(description));
                hosts[host].push_back(caf::actor_cast<caf::actor>(node));
              }
              remote_spawn_count -= 1;
              continue_if_done_spawning();
            },
            [](caf::error& err) {
              VAST_WARN("failed spawn request: {}", err);
              die("todo");
            });
        break;
      }
    }
  }
  continue_if_done_spawning();
}

auto pipeline_executor_state::run() -> caf::result<void> {
  if (!pipe) {
    return caf::make_error(ec::logic_error,
                           fmt::format("{} received run twice", *self));
  }
  auto ops = (*std::exchange(pipe, std::nullopt)).unwrap();
  if (ops.empty())
    return {}; // no-op; empty pipeline
  auto has_remote = std::any_of(ops.begin(), ops.end(), [](auto& op) {
    return op->location() == operator_location::remote;
  });
  rp_complete = self->make_response_promise<void>();
  if (has_remote) {
    system::connect_to_node(
      self, content(self->system().config()),
      // We use a shared_ptr because of non-copyable operator_ptr.
      [this, ops = std::make_shared<decltype(ops)>(std::move(ops))](
        caf::expected<system::node_actor> node) mutable {
        if (!node) {
          rp_complete.deliver(node.error());
          self->quit(node.error());
          return;
        }
        spawn_execution_nodes(*node, std::move(*ops));
      });
  } else {
    spawn_execution_nodes({}, std::move(ops));
  }
  return rp_complete;
}

void pipeline_executor_state::continue_if_done_spawning() {
  if (remote_spawn_count == 0) {
    // We move the actor handles out of the state and do have references to
    // the actors after this function returns. The actors are only kept alive
    // by the ongoing streaming.
    auto flattened = flatten(std::move(hosts));
    VAST_DEBUG("spawning done, starting pipeline with {} actors",
               flattened.size());
    if (flattened.empty()) {
      auto err = caf::make_error(ec::logic_error,
                                 "node returned empty set of execution nodes "
                                 "for remote pipeline");
      rp_complete.deliver(err);
      self->quit(std::move(err));
      return;
    }
    auto source = std::move(flattened.front());
    auto next = std::move(flattened);
    next.erase(next.begin());
    self
      ->request(caf::actor_cast<system::execution_node_actor>(source),
                caf::infinite, atom::run_v, std::move(next))
      .then(
        [=]() {
          VAST_DEBUG("finished pipeline executor initialization");
        },
        [=](caf::error& err) {
          rp_complete.deliver(err);
          self->quit(std::move(err));
        });
  }
}

auto pipeline_executor(
  system::pipeline_executor_actor::stateful_pointer<pipeline_executor_state>
    self,
  pipeline p) -> system::pipeline_executor_actor::behavior_type {
  self->state.self = self;
  self->set_down_handler([self](caf::down_msg& msg) {
    VAST_DEBUG("pipeline executor node down: {}, reason: {}", msg.source,
               msg.reason);
    VAST_ASSERT(self->state.nodes_alive > 0);
    self->state.nodes_alive -= 1;
    auto description = self->state.node_descriptions.find(msg.source);
    VAST_ASSERT(description != self->state.node_descriptions.end(),
                "pipeline executor received down message from unknown "
                "execution node");
    VAST_DEBUG("received down message from '{}': {}", description->second,
               msg.reason);
    if (self->state.rp_complete.pending()) {
      if (msg.reason && msg.reason != caf::exit_reason::unreachable) {
        VAST_DEBUG("delivering error after down: {}", msg.reason);
        self->state.rp_complete.deliver(msg.reason);
        self->quit(msg.reason);
      } else if (self->state.nodes_alive == 0) {
        VAST_DEBUG("all execution nodes are done, delivering success");
        self->state.rp_complete.deliver();
        self->quit();
      } else {
        VAST_DEBUG("not all execution nodes are done, waiting");
      }
    } else {
      VAST_DEBUG("promise ist not pending, discarding down message");
    }
  });
  self->state.pipe = std::move(p);
  return {
    [self](atom::run) -> caf::result<void> {
      return self->state.run();
    },
  };
}

} // namespace vast
