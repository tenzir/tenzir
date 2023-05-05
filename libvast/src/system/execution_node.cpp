//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/execution_node.hpp"

#include "vast/modules.hpp"
#include "vast/operator_control_plane.hpp"

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
    (*self_.state.shutdown)(std::move(error));
  }

  auto warn(caf::error err) noexcept -> void override {
    VAST_WARN("[WARN] {}: {}", self_.state.op->to_string(), err);
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    return vast::modules::schemas();
  }

  auto concepts() const noexcept -> const concepts_map& override {
    return vast::modules::concepts();
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
    : op_(std::move(op)),
      gen_(std::move(gen)),
      self_{host.self},
      shutdown_{host.shutdown} {
  }

  void pull(caf::downstream<Output>& out, size_t num) override {
    // TODO: use num and timeout logic?
    // auto num_events = num * 10000 / 100;
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      // The source will signal that it is exhausted in `done()`, which always
      // gets called afer `pull()`, so we don't need to check it here.
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
      (*shutdown_)({});
    }
    return is_done;
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing source: {}", error);
    (*shutdown_)({});
    self_->send_exit(self_->address(), error);
  }

private:
  // The order here is important. Because the generator is derived from the
  // operator, we want to destroy the operator only after the generator has been
  // destroyed. Thus, `op` must be declared before `gen`. The same holds true
  // for the other stream drivers.
  operator_ptr op_;
  generator<Output> gen_;
  system::execution_node_actor::pointer self_;
  std::shared_ptr<std::function<auto(caf::error)->void>> shutdown_;
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
      self_{host.self},
      shutdown_{host.shutdown} {
  }

  void process(caf::downstream<Output>& out, std::vector<Input>& in) override {
    VAST_DEBUG("stage driver received input ({})", op_->to_string());
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      (*shutdown_)({});
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
        (*shutdown_)({});
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
      (*shutdown_)({});
      self_->send_exit(self_->address(), error);
      return;
    }
    // Run the generator until completion.
    *stop_ = true;
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      (*shutdown_)({});
      self_->send_exit(self_->address(), error);
      return;
    }
    while (true) {
      ++it;
      if (it == gen_.end()) {
        break;
      }
      if (!empty(*it)) {
        this->out_.push(std::move(*it));
      }
    }
    (*shutdown_)({});
    self_->send_exit(self_->address(), error);
  }

private:
  operator_ptr op_;
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<Output> gen_;
  system::execution_node_actor::pointer self_;
  std::shared_ptr<std::function<auto(caf::error)->void>> shutdown_;
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
      self_{host.self},
      shutdown_{host.shutdown} {
  }

  void process(std::vector<Input>& in) override {
    VAST_DEBUG("sink driver received input");
    auto it = gen_.unsafe_current();
    if (it == gen_.end()) {
      (*shutdown_)({});
      return;
    }
    VAST_ASSERT(queue_->empty());
    std::move(in.begin(), in.end(), std::back_inserter(*queue_));
    while (!queue_->empty()) {
      ++it;
      if (it == gen_.end()) {
        (*shutdown_)({});
        return;
      }
    }
  }

  void finalize(const caf::error& error) override {
    VAST_DEBUG("finalizing sink driver");
    if (error) {
      (*shutdown_)({});
      self_->send_exit(self_->address(), error);
      return;
    }
    // Run generator until completion.
    *stop_ = true;
    auto it = gen_.unsafe_current();
    while (it != gen_.end()) {
      ++it;
    }
    (*shutdown_)({});
    self_->send_exit(self_->address(), error);
  }

private:
  operator_ptr op_;
  std::shared_ptr<std::deque<Input>> queue_;
  std::shared_ptr<bool> stop_;
  generator<std::monostate> gen_;
  system::execution_node_actor::pointer self_;
  std::shared_ptr<std::function<auto(caf::error)->void>> shutdown_;
};

template <class Stream>
void shutdown_func(Stream& x, caf::error error) {
  if (!x) {
    if (error) {
      VAST_WARN("execution node ignores error because it is already shutting "
                "down: {}",
                error);
    }
    return;
  }
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
  if (error) {
    x->out().abort(std::move(error));
  } else {
    x->out().close();
  }
  // Finally we call `force_emit_batches()` to move messages from the
  // outbound path buffers to the inboxes of the receiving actors.
  // The 'force_' here means that caf should ignore the batch size
  // and capacity of the channel and push both overfull and underfull
  // batches. Technically just `emit_batches()` would have the same
  // effect since the buffered downstream manager always forces batches
  // if all paths are closing.
  x->out().force_emit_batches();
  // Ensure that we don't call this function again.
  x = nullptr;
}

auto make_shutdown_func() {
  return std::make_shared<std::function<auto(caf::error)->void>>();
}

template <class Stream>
auto emplace_shutdown_func(std::function<auto(caf::error)->void>& fun,
                           Stream& x) {
  fun = [=](caf::error error) mutable {
    return shutdown_func(x, std::move(error));
  };
}

} // namespace

auto execution_node_state::start(std::vector<caf::actor> next)
  -> caf::result<void> {
  if (!op) {
    return caf::make_error(ec::logic_error,
                           fmt::format("{} was already started", *self));
  }
  auto current_op = std::move(op);
  auto result = current_op->instantiate(std::monostate{}, *ctrl);
  if (!result) {
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
        shutdown = make_shutdown_func();
        auto source = caf::detail::make_stream_source<source_driver<T>>(
          self, std::move(current_op), std::move(gen), *this);
        emplace_shutdown_func(*shutdown, source);
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
        shutdown = make_shutdown_func();
        auto sink = caf::detail::make_stream_sink<sink_driver<Input>>(
          self, std::move(current_op), std::move(queue), std::move(stop),
          std::move(gen), *this);
        emplace_shutdown_func(*shutdown, sink);
        return sink->add_inbound_path(in);
      } else {
        if (next.empty()) {
          return caf::make_error(
            ec::logic_error, "pipeline is still open after last operator '{}'",
            current_op->to_string());
        }
        shutdown = make_shutdown_func();
        auto stage
          = caf::detail::make_stream_stage<stage_driver<Input, Output>>(
            self, std::move(current_op), std::move(queue), std::move(stop),
            std::move(gen), *this);
        emplace_shutdown_func(*shutdown, stage);
        auto slot = stage->add_inbound_path(in);
        auto dest = std::move(next.front());
        next.erase(next.begin());
        stage->add_outbound_path(dest, std::make_tuple(std::move(next)));
        return slot;
      }
    },
    std::move(*result));
}

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

} // namespace vast
