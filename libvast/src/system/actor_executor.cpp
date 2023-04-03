//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/actor_executor.hpp"

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

// TODO: This is just a hack.
static operator_control_plane* dummy_ctrl;

class actor_control_plane : public operator_control_plane {
public:
  explicit actor_control_plane(caf::event_based_actor* self) : self_{self} {
    VAST_ASSERT(self_);
  }

  auto self() noexcept -> caf::event_based_actor& override {
    return *self_;
  }

  auto abort(caf::error error) noexcept -> void override {
    VAST_ASSERT(error != caf::none);
    die("not implemented");
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
  caf::event_based_actor* self_;
};

auto empty(const table_slice& slice) -> bool {
  return slice.rows() == 0;
}

auto empty(const chunk_ptr& chunk) -> bool {
  return chunk->size() == 0;
}

template <class Output>
  requires(!std::same_as<Output, std::monostate>)
class source_driver final
  : public caf::stream_source_driver<caf::broadcast_downstream_manager<Output>> {
public:
  source_driver(operator_ptr op, generator<Output> gen,
                actor_control_plane* ctrl)
    : op_(std::move(op)), gen_(std::move(gen)), ctrl_{ctrl} {
    (void)gen_.begin();
  }

  void pull(caf::downstream<Output>& out, size_t num) override {
    // TODO: use num and timeout logic?
    // auto num_events = num * 10000 / 100;
    for (size_t i = 0; i < num; ++i) {
      auto it = gen_.unsafe_current();
      if (it == gen_.end())
        return;
      auto next = std::move(*it);
      ++it;
      if (empty(next))
        return;
      // if (error in ctrl plane)
      //   source->stop(error)
      out.push(std::move(next));
      // TODO consider increment -> done -> move order to avoid unnecessary
      // stalling
    }
  }

  auto done() const noexcept -> bool override {
    auto result = gen_.unsafe_current() == gen_.end();
    if (result) {
      VAST_DEBUG("source is done");
    }
    return result;
  }

  void finalize(const caf::error& err) override {
    // FIXME
    VAST_ASSERT(!err);
    VAST_DEBUG("finalizing source");
  }

private:
  // The order here is important. Because the generator is derived from the
  // operator, we want to destroy the operator only after the generator has been
  // destroyed. Thus, `op` must be declared before `gen`. The same holds true
  // for the other stream drivers.
  operator_ptr op_;
  generator<Output> gen_;
  operator_control_plane* ctrl_;
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
               std::shared_ptr<bool> stop, generator<Output> gen)
    : super{out},
      op_{std::move(op)},
      queue_{std::move(queue)},
      stop_{std::move(stop)},
      gen_{std::move(gen)} {
    (void)gen_.begin();
  }

  void process(caf::downstream<Output>& out, std::vector<Input>& in) override {
    VAST_DEBUG("stage driver received input");
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      // TODO
      return;
    }
    VAST_ASSERT(queue_->empty());
    VAST_ASSERT(std::none_of(in.begin(), in.end(), [](auto& x) {
      return empty(x);
    }));
    std::move(in.begin(), in.end(), std::back_inserter(*queue_));
    while (true) {
      if (it == gen_.end()) {
        return;
      }
      auto batch = std::move(*it);
      ++it;
      if (empty(batch) && queue_->empty()) {
        return;
      }
      if (!empty(batch)) {
        out.push(std::move(batch));
      }
    }
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing stage driver");
    VAST_ASSERT(!error); // <--- TODO
    *stop_ = true;
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      return;
    }
    // `caf::detail::stream_stage_impl::handle`
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
  }

private:
  operator_ptr op_;
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<Output> gen_;
};

// We need this to get access to `out` when finalizing.
template <class Input>
  requires(!std::same_as<Input, std::monostate>)
class sink_driver final : public caf::stream_sink_driver<Input> {
public:
  sink_driver(operator_ptr op, std::shared_ptr<std::deque<Input>> queue,
              std::shared_ptr<bool> stop, generator<std::monostate> gen)
    : op_{std::move(op)},
      queue_{std::move(queue)},
      stop_{std::move(stop)},
      gen_{std::move(gen)} {
    (void)gen_.begin();
  }

  void process(std::vector<Input>& in) override {
    VAST_DEBUG("sink driver received input");
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      // TODO
      return;
    }
    VAST_ASSERT(queue_->empty());
    std::move(in.begin(), in.end(), std::back_inserter(*queue_));
    while (it != gen_.end() && !queue_->empty()) {
      ++it;
    }
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing sink driver");
    VAST_ASSERT(!error); // <--- TODO
    *stop_ = true;
    auto it = gen_.unsafe_current();
    while (it != gen_.end()) {
      ++it;
    }
  }

private:
  operator_ptr op_;
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<std::monostate> gen_;
};

auto execution_node(
  system::execution_node_actor::stateful_pointer<execution_node_state> self,
  operator_ptr op) -> system::execution_node_actor::behavior_type {
  self->state.self = self;
  self->state.op = std::move(op);
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

template <typename T>
auto flatten(std::vector<std::vector<T>> vecs) -> std::vector<T> {
  auto result = std::vector<T>{};
  for (auto& vec : vecs) {
    result.insert(result.end(), std::move_iterator{vec.begin()},
                  std::move_iterator{vec.end()});
  }
  return result;
}

/// The interface of a PIPELINE EXECUTOR actor.
using pipeline_executor_actor = system::typed_actor_fwd<
  // Execute a logical pipeline, returning the result asynchronously.
  auto(atom::run)->caf::result<void>>::unwrap;

struct pipeline_executor_state {
  static constexpr auto name = "pipeline-executor";

  pipeline_executor_actor::pointer self;
  std::optional<pipeline> pipe;
  size_t nodes_alive{0};
  caf::typed_response_promise<void> rp_complete;
  std::vector<std::vector<caf::actor>> hosts;
  size_t remote_spawn_count{0};

  void continue_if_done_spawning() {
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
            rp_complete.deliver(std::move(err));
          });
    }
  }

  void spawn_execution_nodes(system::node_actor remote,
                             std::vector<operator_ptr> ops) {
    VAST_DEBUG("spawning execution nodes (remote = {})", remote);
    hosts.reserve(ops.size());
    for (auto it = ops.begin(); it != ops.end(); ++it) {
      switch ((*it)->location()) {
        case operator_location::local:
        case operator_location::anywhere: {
          // Spawn and collect execution nodes until the first remote operator.
          auto& v = hosts.emplace_back();
          while (true) {
            v.push_back(caf::actor_cast<caf::actor>(
              self->spawn<caf::monitored>(execution_node, std::move(*it))));
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
          die("todo");
          auto begin = it;
          ++it;
          while (it != ops.end()) {
            if ((*it)->location() == operator_location::local) {
              break;
            }
          }
          auto end = it;
          --it;
          VAST_ASSERT(remote);
          auto subpipe
            = pipeline{{std::move_iterator{begin}, std::move_iterator{it}}};
          // Allocate a slot in `hosts`, saving its index.
          auto host = hosts.size();
          hosts.emplace_back();
          // We keep track of the remote spawning calls in order to continue
          // only after remoting spawning is complete.
          remote_spawn_count += 1;
          self
            ->request(remote, caf::infinite, atom::spawn_v, std::move(subpipe))
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
                  hosts[host].push_back(caf::actor_cast<caf::actor>(node));
                }
                remote_spawn_count -= 1;
                continue_if_done_spawning();
              },
              [](caf::error&) {
                die("todo");
              });
          break;
        }
      }
    }
    continue_if_done_spawning();
  }

  auto run() -> caf::result<void> {
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
    rp_complete = self->make_response_promise<void>(); // TODO
    if (has_remote) {
      system::connect_to_node(
        self, content(self->system().config()),
        // TODO: shared_ptr because of non-copyable ops
        [this, ops = std::make_shared<decltype(ops)>(std::move(ops))](
          caf::expected<system::node_actor> node) mutable {
          if (!node) {
            rp_complete.deliver(node.error());
            return;
          }
          spawn_execution_nodes(*node, std::move(*ops));
        });
    } else {
      spawn_execution_nodes({}, std::move(ops));
    }
    return rp_complete;
  }
};

auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline p) -> pipeline_executor_actor::behavior_type {
  self->state.self = self;
  self->set_down_handler([self](caf::down_msg& msg) {
    VAST_DEBUG("pipeline executor node down: {}", msg.source);
    VAST_ASSERT(self->state.nodes_alive > 0);
    self->state.nodes_alive -= 1;
    if (self->state.rp_complete.pending()) {
      if (msg.reason != caf::exit_reason::unreachable) {
        VAST_DEBUG("delivering error after down: {}", msg.reason);
        self->state.rp_complete.deliver(msg.reason);
      } else if (self->state.nodes_alive == 0) {
        VAST_DEBUG("all execution nodes are done, delivering success");
        self->state.rp_complete.deliver();
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

} // namespace

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
  auto actor_ptr
    = caf::actor_cast<caf::event_based_actor*>(self); // TODO: Safe?
  ctrl
    = std::make_unique<actor_control_plane>(actor_ptr); // TODO: Check lifetime
  auto result = current_op->instantiate(std::monostate{}, *ctrl);
  if (!result) {
    die("todo");
  }
  return std::visit(
    detail::overload{
      [&](generator<std::monostate>&) -> caf::result<void> {
        // This case corresponds to a `void -> void` operator.
        if (!next.empty()) {
          return caf::make_error(ec::logic_error, "TODOs");
        }
        // TODO: void -> void not implemented
        return caf::make_error(ec::unimplemented,
                               "support for void -> void operators is not "
                               "implemented yet");
      },
      [&]<class T>(generator<T>& gen) -> caf::result<void> {
        if (next.empty()) {
          return caf::make_error(ec::logic_error, "TODO");
        }
        auto source = caf::detail::make_stream_source<source_driver<T>>(
          self, std::move(current_op), std::move(gen), &*ctrl);
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
    = current_op->instantiate(generator_for_queue(queue, stop), *dummy_ctrl);
  if (!result) {
    die("todo");
  }
  return std::visit(
    [&]<class Output>(
      generator<Output> gen) -> caf::expected<caf::inbound_stream_slot<Input>> {
      if constexpr (std::is_same_v<Output, std::monostate>) {
        if (!next.empty()) {
          return caf::make_error(ec::logic_error, "TODO");
        }
        auto sink = caf::detail::make_stream_sink<sink_driver<Input>>(
          self, std::move(current_op), std::move(queue), std::move(stop),
          std::move(gen));
        return sink->add_inbound_path(in);
      } else {
        if (next.empty()) {
          return caf::make_error(ec::logic_error, "TODO");
        }
        auto stage
          = caf::detail::make_stream_stage<stage_driver<Input, Output>>(
            self, std::move(current_op), std::move(queue), std::move(stop),
            std::move(gen));
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
