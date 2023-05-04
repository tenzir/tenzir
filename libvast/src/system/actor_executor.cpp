//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/actor_executor.hpp"

#include "vast/aliases.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/operator_control_plane.hpp"
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

class actor_control_plane : public operator_control_plane {
public:
  explicit actor_control_plane(
    system::execution_node_actor::stateful_impl<execution_node_state>& self)
    : self_{self} {
  }

  auto self() noexcept -> caf::scheduled_actor& override {
    return self_;
  }

  auto abort(caf::error error) noexcept -> void override {
    VAST_DEBUG("{} called actor_control_plane::abort({})", self_, error);
    VAST_ASSERT(error != caf::none);
    self_.state.shutdown(self_.state, std::move(error));
  }

  auto warn(caf::error) noexcept -> void override {
    die("not implemented");
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto demand(type = {}) const noexcept -> size_t override {
    die("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    die("not implemented");
  }

  auto concepts() const noexcept -> const concepts_map& override {
    die("not implemented");
  }

private:
  system::execution_node_actor::stateful_impl<execution_node_state>& self_;
};

auto empty(const table_slice& slice) -> bool {
  return slice.rows() == 0;
}

auto empty(const chunk_ptr& chunk) -> bool {
  return !chunk || chunk->size() == 0;
}

template <class Output>
  requires(!std::same_as<Output, std::monostate>)
class source_driver final
  : public caf::stream_source_driver<caf::broadcast_downstream_manager<Output>> {
public:
  source_driver(operator_ptr op, generator<Output> gen,
                execution_node_state& host)
    : op_(std::move(op)), gen_(std::move(gen)), host_{host} {
  }

  void pull(caf::downstream<Output>& out, size_t num) override {
    // TODO: use num and timeout logic?
    // auto num_events = num * 10000 / 100;
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      // TODO: Check this.
      // The source will signal that it is exhausted in `done()`.
      return;
    }
    for (size_t i = 0; i < num; ++i) {
      ++it;
      if (it == gen_.end()) {
        return;
      }
      auto next = std::move(*it);
      if (empty(next))
        return;
      out.push(std::move(next));
    }
  }

  auto done() const noexcept -> bool override {
    auto is_done = gen_.unsafe_current() == gen_.end();
    if (is_done) {
      VAST_DEBUG("source is done");
      host_.shutdown(host_, {});
    }
    return is_done;
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing source: {}", error);
    host_.shutdown(host_, error);
    host_.self->quit(error);
  }

private:
  // The order here is important. Because the generator is derived from the
  // operator, we want to destroy the operator only after the generator has been
  // destroyed. Thus, `op` must be declared before `gen`. The same holds true
  // for the other stream drivers.
  operator_ptr op_;
  generator<Output> gen_;
  execution_node_state& host_;
};

template <class Input>
auto generator_for_queue(std::shared_ptr<std::deque<Input>> queue,
                         std::shared_ptr<bool> stop) -> generator<Input> {
  VAST_ASSERT(queue);
  while (!*stop) {
    if (queue->empty()) {
      co_yield Input{};
      continue;
    }
    auto batch = std::move(queue->front());
    queue->pop_front();
    // Empty batches should not be put in any queue, but handled directly.
    VAST_ASSERT(!empty(batch));
    co_yield std::move(batch);
  }
}

// We need a custom driver to get access to `out` when finalizing.
template <class Input, class Output>
  requires(!std::same_as<Input, std::monostate>
           && !std::same_as<Output, std::monostate>)
class stage_driver final
  : public caf::stream_stage_driver<Input,
                                    caf::broadcast_downstream_manager<Output>> {
  using super
    = caf::stream_stage_driver<Input, caf::broadcast_downstream_manager<Output>>;

public:
  stage_driver(caf::broadcast_downstream_manager<Output>& out, operator_ptr op,
               std::shared_ptr<std::deque<Input>> queue,
               std::shared_ptr<bool> stop, generator<Output> gen,
               execution_node_state& host)
    : super{out},
      op_{std::move(op)},
      queue_{std::move(queue)},
      stop_{std::move(stop)},
      gen_{std::move(gen)},
      host_{host} {
  }

  void process(caf::downstream<Output>& out, std::vector<Input>& in) override {
    VAST_DEBUG("stage driver received input ({})", op_->to_string());
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      host_.shutdown(host_, {});
      return;
    }
    VAST_ASSERT(queue_->empty());
    VAST_ASSERT(std::none_of(in.begin(), in.end(), [](auto& x) {
      return empty(x);
    }));
    std::move(in.begin(), in.end(), std::back_inserter(*queue_));
    while (true) {
      ++it;
      if (it == gen_.end()) {
        host_.shutdown(host_, {});
        return;
      }
      auto batch = std::move(*it);
      if (empty(batch) && queue_->empty()) {
        return;
      }
      if (!empty(batch)) {
        out.push(std::move(batch));
      }
    }
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing stage driver for ({}), error = {}", op_->to_string(),
               error);
    if (error) {
      // If there is an error, we drop the generator without running it to
      // completion.
      host_.shutdown(host_, error);
      return;
    }
    // Run the generator until completion.
    *stop_ = true;
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      host_.shutdown(host_, {});
      return;
    }
    while (true) {
      ++it;
      if (it == gen_.end()) {
        break;
      }
      if (!empty(*it)) {
        // TODO: Is this legal?
        this->out_.push(std::move(*it));
      }
    }
    // TODO: There could have already been an error. Is this okay?
    host_.shutdown(host_, {});
    host_.self->quit(error);
  }

private:
  operator_ptr op_;
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<Output> gen_;
  execution_node_state& host_;
};

// We need this to get access to `out` when finalizing.
template <class Input>
  requires(!std::same_as<Input, std::monostate>)
class sink_driver final : public caf::stream_sink_driver<Input> {
public:
  sink_driver(operator_ptr op, std::shared_ptr<std::deque<Input>> queue,
              std::shared_ptr<bool> stop, generator<std::monostate> gen,
              execution_node_state& host)
    : op_{std::move(op)},
      queue_{std::move(queue)},
      stop_{std::move(stop)},
      gen_{std::move(gen)},
      host_{host} {
  }

  void process(std::vector<Input>& in) override {
    VAST_DEBUG("sink driver received input");
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      host_.shutdown(host_, {});
      return;
    }
    VAST_ASSERT(queue_->empty());
    std::move(in.begin(), in.end(), std::back_inserter(*queue_));
    while (!queue_->empty()) {
      ++it;
      if (it == gen_.end()) {
        host_.shutdown(host_, {});
        return;
      }
    }
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing sink driver");
    if (error) {
      host_.shutdown(host_, error);
      return;
    }
    // Run generator until completion.
    *stop_ = true;
    auto it = gen_.unsafe_current();
    while (it != gen_.end()) {
      ++it;
    }
    host_.shutdown(host_, {});
    host_.self->quit(error);
  }

private:
  operator_ptr op_;
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<std::monostate> gen_;
  execution_node_state& host_;
};

template <typename T>
auto flatten(std::vector<std::vector<T>> vecs) -> std::vector<T> {
  auto result = std::vector<T>{};
  for (auto& vec : vecs) {
    result.insert(result.end(), std::move_iterator{vec.begin()},
                  std::move_iterator{vec.end()});
  }
  return result;
}

template <class Self, class Stream, class State>
void shutdown_func(Self self, Stream& x, State& state, caf::error error) {
  auto first_shutdown = !state.is_shutting_down;
  state.is_shutting_down = true;
  x->shutdown();
  // Then, we copy all data from the global input buffer to each
  // path-specific output buffer.
  if constexpr (requires { x->out().fan_out_flush(); }) {
    x->out().fan_out_flush();
  }
  // Next, we `close()` the outbound paths to notify downstream
  // connections that this stage is closed.
  // This will remove all clean outbound paths, so we need to call
  // `fan_out_flush()` before, but it will keep all paths that still
  // have data. No new data will be pushed from the global buffer to
  // closing paths.
  x->out().close();
  // Finally we call `force_emit_batches()` to move messages from the
  // outbound path buffers to the inboxes of the receiving actors.
  // The 'force_' here means that caf should ignore the batch size
  // and capacity of the channel and push both overfull and underfull
  // batches. Technically just `emit_batches()` would have the same
  // effect since the buffered downstream manager always forces batches
  // if all paths are closing.
  x->out().force_emit_batches();
  if (first_shutdown && error) {
    self->quit(std::move(error));
  }
}

} // namespace

auto execution_node(
  system::execution_node_actor::stateful_pointer<execution_node_state> self,
  operator_ptr op) -> system::execution_node_actor::behavior_type {
  self->state.self = self;
  self->state.op = std::move(op);
  self->state.ctrl = std::make_unique<actor_control_plane>(*self);
  return {
    [self](atom::run, std::vector<caf::actor> next) -> caf::result<void> {
      VAST_DEBUG("source execution node received atom::run");
      return self->state.start(std::move(next));
    },
    [self](caf::stream<table_slice> in, std::vector<caf::actor> next) {
      return self->state.start(in, std::move(next));
    },
    [self](caf::stream<chunk_ptr> in, std::vector<caf::actor> next) {
      return self->state.start(in, std::move(next));
    },
  };
}

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
              self->spawn<caf::monitored + caf::detached>(execution_node,
                                                          std::move(*it))));
          } else {
            v.push_back(caf::actor_cast<caf::actor>(
              self->spawn<caf::monitored>(execution_node, std::move(*it))));
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
            [=, this](std::vector<system::execution_node_actor>
                        execution_nodes) mutable {
              // TODO: pipeline serialization
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
              for (auto& node : execution_nodes) {
                self->monitor(node);
                nodes_alive += 1;
                // FIXME: Make the node return this as well.
                node_descriptions.emplace(node.address(), "<remote>");
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
      // TODO: shared_ptr because of non-copyable ops
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
    VAST_ASSERT(!flattened.empty()); // TODO
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

void start_actor_executor(caf::event_based_actor* self, pipeline p,
                          std::function<void(caf::expected<void>)> callback) {
  VAST_DEBUG("spawning actor executor");
  auto executor = self->spawn(pipeline_executor, std::move(p));
  self->request(executor, caf::infinite, atom::run_v)
    .then(
      // We capture to `executor` here because the current implementation does
      // not keep the executor alive until the request has completed.
      [=] {
        (void)executor;
        VAST_DEBUG("actor executor done");
        callback({});
      },
      [=](caf::error& error) {
        (void)executor;
        VAST_DEBUG("actor executor error: {}", error);
        callback(std::move(error));
      });
}

auto execution_node_state::start(std::vector<caf::actor> next)
  -> caf::result<void> {
  if (!op) {
    return caf::make_error(ec::logic_error,
                           fmt::format("{} was already started", *self));
  }
  auto current_op = std::move(op);
  auto result = current_op->instantiate(std::monostate{}, *ctrl);
  if (!result) {
    // TODO: Is this the right way to do it?
    self->quit(std::move(result.error()));
    return {};
  }
  return std::visit(
    detail::overload{
      [&](generator<std::monostate>&) -> caf::result<void> {
        // This case corresponds to a `void -> void` operator.
        if (!next.empty()) {
          return caf::make_error(
            ec::logic_error,
            fmt::format("pipeline was already closed by '{}', but has more "
                        "operators ({}) afterwards",
                        current_op->to_string(), next.size()));
        }
        // TODO: void -> void not implemented
        return caf::make_error(ec::unimplemented,
                               "support for void -> void operators is not "
                               "implemented yet");
      },
      [&]<class T>(generator<T>& gen) -> caf::result<void> {
        if (next.empty()) {
          return caf::make_error(
            ec::logic_error, "pipeline is still open after last operator '{}'",
            current_op->to_string());
        }
        auto source = caf::detail::make_stream_source<source_driver<T>>(
          self, std::move(current_op), std::move(gen), *this);
        shutdown = [=](execution_node_state& state, caf::error error) {
          shutdown_func(self, source, state, error);
        };
        auto dest = std::move(next.front());
        next.erase(next.begin());
        source->add_outbound_path(dest, std::make_tuple(std::move(next)));
        return {};
      }},
    *result);
}

template <class Input>
auto execution_node_state::start(caf::stream<Input> in,
                                 std::vector<caf::actor> next)
  -> caf::result<caf::inbound_stream_slot<Input>> {
  if (!op) {
    return caf::make_error(ec::logic_error,
                           fmt::format("{} was already started", *self));
  }
  auto current_op = std::move(op);
  auto queue = std::make_shared<std::deque<Input>>();
  auto stop = std::make_shared<bool>();
  auto result
    = current_op->instantiate(generator_for_queue(queue, stop), *ctrl);
  if (!result) {
    // TODO: Is this the right way to do it?
    self->quit(std::move(result.error()));
    return {};
  }
  return std::visit(
    [&]<class Output>(
      generator<Output> gen) -> caf::expected<caf::inbound_stream_slot<Input>> {
      if constexpr (std::is_same_v<Output, std::monostate>) {
        if (!next.empty()) {
          return caf::make_error(
            ec::logic_error,
            fmt::format("pipeline was already closed by '{}', but has more "
                        "operators ({}) afterwards",
                        current_op->to_string(), next.size()));
        }
        auto sink = caf::detail::make_stream_sink<sink_driver<Input>>(
          self, std::move(current_op), std::move(queue), std::move(stop),
          std::move(gen), *this);
        shutdown = [=](execution_node_state& state, caf::error error) {
          shutdown_func(self, sink, state, error);
        };
        return sink->add_inbound_path(in);
      } else {
        if (next.empty()) {
          return caf::make_error(
            ec::logic_error, "pipeline is still open after last operator '{}'",
            current_op->to_string());
        }
        auto stage
          = caf::detail::make_stream_stage<stage_driver<Input, Output>>(
            self, std::move(current_op), std::move(queue), std::move(stop),
            std::move(gen), *this);
        shutdown = [=](execution_node_state& state, caf::error error) {
          shutdown_func(self, stage, state, error);
        };
        auto slot = stage->add_inbound_path(in);
        auto dest = std::move(next.front());
        next.erase(next.begin());
        stage->add_outbound_path(dest, std::make_tuple(std::move(next)));
        return slot;
      }
    },
    std::move(*result));
}

} // namespace vast
